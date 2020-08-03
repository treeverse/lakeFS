package gateway

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/dedup"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/sig"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/stats"
)

type handler struct {
	BareDomain         string
	sc                 *ServerContext
	operationID        string
	NotFoundHandler    http.Handler
	ServerErrorHandler http.Handler
}

type ServerContext struct {
	ctx          context.Context
	region       string
	bareDomain   string
	cataloger    catalog.Cataloger
	blockStore   block.Adapter
	authService  simulator.GatewayAuthService
	stats        stats.Collector
	dedupCleaner *dedup.Cleaner
}

func (c *ServerContext) WithContext(ctx context.Context) *ServerContext {
	return &ServerContext{
		ctx:          ctx,
		region:       c.region,
		bareDomain:   c.bareDomain,
		cataloger:    c.cataloger,
		blockStore:   c.blockStore.WithContext(ctx),
		authService:  c.authService,
		stats:        c.stats,
		dedupCleaner: c.dedupCleaner,
	}
}

func NewHandler(
	region string,
	cataloger catalog.Cataloger,
	blockStore block.Adapter,
	authService simulator.GatewayAuthService,
	bareDomain string,
	stats stats.Collector,
	dedupCleaner *dedup.Cleaner,
) http.Handler {
	sc := &ServerContext{
		ctx:          context.Background(),
		cataloger:    cataloger,
		region:       region,
		bareDomain:   bareDomain,
		blockStore:   blockStore,
		authService:  authService,
		stats:        stats,
		dedupCleaner: dedupCleaner,
	}

	// setup routes
	var h http.Handler
	h = &handler{
		BareDomain:         bareDomain,
		sc:                 sc,
		NotFoundHandler:    http.HandlerFunc(notFound),
		ServerErrorHandler: nil,
	}
	h = simulator.RegisterRecorder(httputil.LoggingMiddleware(
		"X-Amz-Request-Id", logging.Fields{"service_name": "s3_gateway"}, h,
	), authService, region, bareDomain)

	logging.Default().WithFields(logging.Fields{
		"s3_bare_domain": bareDomain,
		"s3_region":      region,
	}).Info("initialized S3 Gateway handler")

	return h
}

func getAPIErrOrDefault(err error, defaultAPIErr gatewayerrors.APIErrorCode) gatewayerrors.APIError {
	apiError, ok := err.(*gatewayerrors.APIErrorCode)
	if ok {
		return apiError.ToAPIErr()
	} else {
		return defaultAPIErr.ToAPIErr()
	}
}

func authenticateOperation(s *ServerContext, writer http.ResponseWriter, request *http.Request, perms []permissions.Permission) *operations.AuthenticatedOperation {
	o := &operations.Operation{
		Request:        request,
		ResponseWriter: writer,
		Region:         s.region,
		FQDN:           s.bareDomain,
		Cataloger:      s.cataloger,
		BlockStore:     s.blockStore,
		Auth:           s.authService,
		Incr: func(action string) {
			logging.FromContext(request.Context()).
				WithField("action", action).
				WithField("message_type", "action").
				Debug("performing S3 action")
			s.stats.CollectEvent("s3_gateway", action)
		},
		DedupCleaner: s.dedupCleaner,
	}

	// authenticate
	authenticator := sig.ChainedAuthenticator(
		sig.NewV4Authenticator(request),
		sig.NewV2SigAuthenticator(request))

	authContext, err := authenticator.Parse()
	if err != nil {
		o.Log().WithError(err).Warn("failed to parse signature")
		o.EncodeError(getAPIErrOrDefault(err, gatewayerrors.ErrAccessDenied))
		return nil
	}
	creds, err := s.authService.GetCredentials(authContext.GetAccessKeyID())
	if err != nil {
		if !errors.Is(err, db.ErrNotFound) {
			o.Log().WithError(err).WithField("key", authContext.GetAccessKeyID()).Warn("error getting access key")
			o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		} else {
			o.Log().WithError(err).WithField("key", authContext.GetAccessKeyID()).Warn("could not find access key")
			o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrAccessDenied))
		}
		return nil
	}

	err = authenticator.Verify(creds, s.bareDomain)
	if err != nil {
		o.Log().WithError(err).WithFields(logging.Fields{
			"key":           authContext.GetAccessKeyID(),
			"authenticator": authenticator,
		}).Warn("error verifying credentials for key")
		o.EncodeError(getAPIErrOrDefault(err, gatewayerrors.ErrAccessDenied))
		return nil
	}

	user, err := s.authService.GetUserByID(creds.UserID)
	if err != nil {
		o.Log().WithError(err).WithFields(logging.Fields{
			"key":           authContext.GetAccessKeyID(),
			"authenticator": authenticator,
		}).Warn("could not get user for credentials key")
		o.EncodeError(getAPIErrOrDefault(err, gatewayerrors.ErrAccessDenied))
		return nil
	}

	// we are verified!
	op := &operations.AuthenticatedOperation{
		Operation: o,
		Principal: user.DisplayName,
	}

	op.AddLogFields(logging.Fields{"user": user.DisplayName})

	if perms == nil {
		// no special permissions required, no need to authorize (used for delete-objects, where permissions are checked separately)
		return op
	}
	// authorize
	authResp, err := s.authService.Authorize(&auth.AuthorizationRequest{
		UserDisplayName:     op.Principal,
		RequiredPermissions: perms,
	})
	if err != nil {
		o.Log().WithError(err).Error("failed to authorize")
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
		return nil
	}

	if authResp.Error != nil || !authResp.Allowed {
		o.Log().WithError(authResp.Error).WithField("key", authContext.GetAccessKeyID()).Warn("no permission")
		o.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrAccessDenied))
		return nil
	}

	// authentication and authorization complete!
	return op
}

func operation(sc *ServerContext, writer http.ResponseWriter, request *http.Request) *operations.Operation {
	return &operations.Operation{
		Request:        request,
		ResponseWriter: writer,
		Region:         sc.region,
		FQDN:           sc.bareDomain,
		Cataloger:      sc.cataloger,
		BlockStore:     sc.blockStore,
		Auth:           sc.authService,
		Incr: func(action string) {
			logging.FromContext(request.Context()).
				WithField("action", action).
				WithField("message_type", "action").
				Debug("performing S3 action")
			sc.stats.CollectEvent("s3_gateway", action)
		},
		DedupCleaner: sc.dedupCleaner,
	}
}

func OperationHandler(sc *ServerContext, handler operations.AuthenticatedOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		perms, err := handler.RequiredPermissions(request)
		if err != nil {
			o := operation(sc, writer, request)
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authenticateOperation(sc.WithContext(request.Context()), writer, request, perms)
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(authOp)
	})
}

func RepoOperationHandler(sc *ServerContext, repoID string, handler operations.RepoOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		perms, err := handler.RequiredPermissions(request, repoID)
		if err != nil {
			o := operation(sc, writer, request)
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authenticateOperation(sc.WithContext(request.Context()), writer, request, perms)
		if authOp == nil {
			return
		}

		// validate repo exists
		repo, err := authOp.Cataloger.GetRepository(sc.ctx, repoID)
		if errors.Is(err, db.ErrNotFound) {
			authOp.Log().WithField("repository", repoID).Warn("the specified repo does not exist")
			authOp.EncodeError(gatewayerrors.ErrNoSuchBucket.ToAPIErr())
			return
		}
		if err != nil {
			authOp.EncodeError(gatewayerrors.ErrInternalError.ToAPIErr())
			return
		}
		// run callback
		repoOperation := &operations.RepoOperation{
			AuthenticatedOperation: authOp,
			Repository:             repo,
		}
		repoOperation.AddLogFields(logging.Fields{
			"repository": repo.Name,
		})
		handler.Handle(repoOperation)
	})
}

func PathOperationHandler(sc *ServerContext, repoID, refID, path string, handler operations.PathOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		perms, err := handler.RequiredPermissions(request, repoID, refID, path)
		if err != nil {
			o := operation(sc, writer, request)
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authenticateOperation(sc.WithContext(request.Context()), writer, request, perms)
		if authOp == nil {
			return
		}

		// validate repo exists
		repo, err := authOp.Cataloger.GetRepository(sc.ctx, repoID)
		if errors.Is(err, db.ErrNotFound) {
			authOp.Log().WithField("repository", repoID).Warn("the specified repo does not exist")
			authOp.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrNoSuchBucket))
			return
		}
		if err != nil {
			authOp.EncodeError(gatewayerrors.Codes.ToAPIErr(gatewayerrors.ErrInternalError))
			return
		}

		// run callback
		operation := &operations.PathOperation{
			RefOperation: &operations.RefOperation{
				RepoOperation: &operations.RepoOperation{
					AuthenticatedOperation: authOp,
					Repository:             repo,
				},
				Reference: refID,
			},
			Path: path,
		}
		operation.AddLogFields(logging.Fields{
			"repository": repo.Name,
			"ref":        refID,
			"path":       path,
		})
		handler.Handle(operation)
	})
}

func unsupportedOperationHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		o := &operations.Operation{
			Request:        request,
			ResponseWriter: writer,
		}
		o.EncodeError(gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
	})
}

func notFound(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// pprof endpoints
	handler := h.servePathBased(r)
	if handler == nil {
		handler = h.serveVirtualHost(r)
	}
	if handler == nil {
		handler = h.NotFoundHandler
	}
	start := time.Now()
	mrw := httputil.NewMetricResponseWriter(w)
	handler.ServeHTTP(mrw, r)
	requestHistograms.WithLabelValues(h.operationID, strconv.Itoa(mrw.StatusCode)).Observe(time.Since(start).Seconds())
}

func (h *handler) servePathBased(r *http.Request) http.Handler {
	host := httputil.HostOnly(r.Host)
	if !strings.EqualFold(host, httputil.HostOnly(h.BareDomain)) {
		return nil // maybe it's a virtual host, but def not a path based request because the host is wrong
	}

	if parts, ok := SplitFirst(r.URL.Path, 3); ok {
		repository := parts[0]
		ref := parts[1]
		key := parts[2]
		if err := catalog.Validate(catalog.ValidateFields{
			{Name: "repository", IsValid: catalog.ValidateRepositoryName(repository)},
			{Name: "reference", IsValid: catalog.ValidateReference(ref)},
			{Name: "path", IsValid: catalog.ValidatePath(key)},
		}); err != nil {
			return h.NotFoundHandler
		}

		return h.pathBasedHandler(r.Method, repository, ref, key)
	}

	// paths for repository and ref only (none exist)
	if parts, ok := SplitFirst(r.URL.Path, 2); ok {
		repository := parts[0]
		ref := parts[1]

		// s3 allows trailing slash for bucket name
		if ref == "" {
			return h.repositoryBasedHandlerIfValid(r.Method, repository)
		}
		return h.NotFoundHandler
	}

	if parts, ok := SplitFirst(r.URL.Path, 1); ok {
		// Paths for bare repository
		repository := parts[0]
		return h.repositoryBasedHandlerIfValid(r.Method, repository)
	}
	// no repository given
	if r.Method == http.MethodGet {
		h.operationID = "list_buckets"
		return OperationHandler(h.sc, &operations.ListBuckets{})
	}
	h.operationID = "not_found_operation"
	return h.NotFoundHandler
}

func (h *handler) serveVirtualHost(r *http.Request) http.Handler {
	// is it a virtual host?
	host := httputil.HostOnly(r.Host)
	ourHost := httputil.HostOnly(h.BareDomain)
	if !strings.HasSuffix(host, ourHost) {
		return nil
	}

	// remove bare domain suffix
	repository := strings.TrimSuffix(host, "."+ourHost)
	if !catalog.IsValidRepositoryName(repository) {
		return h.NotFoundHandler
	}

	// Paths that have both a repository, a refId and a path
	if parts, ok := SplitFirst(r.URL.Path, 2); ok {
		// validate ref, key
		ref := parts[0]
		key := parts[1]
		if err := catalog.Validate(catalog.ValidateFields{
			{Name: "reference", IsValid: catalog.ValidateReference(ref)},
			{Name: "path", IsValid: catalog.ValidatePath(key)},
		}); err != nil {
			return h.NotFoundHandler
		}
		return h.pathBasedHandler(r.Method, repository, ref, key)
	}

	// Paths that only have a repository and a refId (always 404)
	if _, ok := SplitFirst(r.URL.Path, 1); ok {
		return h.NotFoundHandler
	}

	return h.repositoryBasedHandler(r.Method, repository)
}

func (h *handler) pathBasedHandler(method, repository, ref, path string) http.Handler {
	var handler operations.PathOperationHandler
	switch method {
	case http.MethodDelete:
		handler = &operations.DeleteObject{}
	case http.MethodPost:
		handler = &operations.PostObject{}
	case http.MethodGet:
		handler = &operations.GetObject{}
	case http.MethodHead:
		handler = &operations.HeadObject{}
	case http.MethodPut:
		handler = &operations.PutObject{}
	default:
		h.operationID = "not_found_operation"
		return h.NotFoundHandler
	}
	h.operationID = reflect.TypeOf(handler).Elem().Name()
	return PathOperationHandler(h.sc, repository, ref, path, handler)
}

func (h *handler) repositoryBasedHandlerIfValid(method, repository string) http.Handler {
	if !catalog.IsValidRepositoryName(repository) {
		return h.NotFoundHandler
	}

	return h.repositoryBasedHandler(method, repository)
}

func (h *handler) repositoryBasedHandler(method, repository string) http.Handler {
	var handler operations.RepoOperationHandler
	switch method {
	case http.MethodDelete, http.MethodPut:
		h.operationID = "unsupported_operation"
		return unsupportedOperationHandler()
	case http.MethodHead:
		handler = &operations.HeadBucket{}
	case http.MethodPost:
		handler = &operations.DeleteObjects{}
	case http.MethodGet:
		handler = &operations.ListObjects{}
	default:
		h.operationID = "not_found_operation"
		return h.NotFoundHandler
	}
	h.operationID = reflect.TypeOf(handler).Elem().Name()

	return RepoOperationHandler(h.sc, repository, handler)
}

func SplitFirst(pth string, parts int) ([]string, bool) {
	pth = strings.TrimPrefix(pth, path.Separator)
	pathParts := strings.SplitN(pth, path.Separator, parts)
	if len(pathParts) < parts {
		return []string{}, false
	}
	if len(pathParts) == 1 && len(pathParts[0]) == 0 {
		return []string{}, false
	}
	return pathParts, true
}
