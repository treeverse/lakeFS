package gateway

import (
	"context"
	"errors"
	"net/http"
	gohttputil "net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/dedup"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/multiparts"
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
	ctx               context.Context
	region            string
	bareDomain        string
	cataloger         catalog.Cataloger
	multipartsTracker multiparts.Tracker
	blockStore        block.Adapter
	authService       simulator.GatewayAuthService
	stats             stats.Collector
	dedupCleaner      *dedup.Cleaner
	fallbackProxy     *gohttputil.ReverseProxy
}

func (c *ServerContext) WithContext(ctx context.Context) *ServerContext {
	return &ServerContext{
		ctx:               ctx,
		region:            c.region,
		bareDomain:        c.bareDomain,
		cataloger:         c.cataloger,
		multipartsTracker: c.multipartsTracker,
		blockStore:        c.blockStore.WithContext(ctx),
		authService:       c.authService,
		stats:             c.stats,
		dedupCleaner:      c.dedupCleaner,
		fallbackProxy:     c.fallbackProxy,
	}
}

func NewHandler(
	region string,
	cataloger catalog.Cataloger,
	multipartsTracker multiparts.Tracker,
	blockStore block.Adapter,
	authService simulator.GatewayAuthService,
	bareDomain string,
	stats stats.Collector,
	dedupCleaner *dedup.Cleaner,
	fallbackURL *url.URL,
) http.Handler {
	var fallbackProxy *gohttputil.ReverseProxy
	if fallbackURL != nil {
		fallbackProxy = gohttputil.NewSingleHostReverseProxy(fallbackURL)
	}
	sc := &ServerContext{
		ctx:               context.Background(),
		cataloger:         cataloger,
		multipartsTracker: multipartsTracker,
		region:            region,
		bareDomain:        bareDomain,
		blockStore:        blockStore,
		authService:       authService,
		stats:             stats,
		dedupCleaner:      dedupCleaner,
		fallbackProxy:     fallbackProxy,
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

func authorize(authContext sig.SigContext, s *ServerContext, writer http.ResponseWriter, request *http.Request, user *model.User, perms []permissions.Permission) *operations.AuthenticatedOperation {
	o := operation(s, writer, request)
	// we are verified!
	op := &operations.AuthenticatedOperation{
		Operation: o,
		Principal: user.Username,
	}

	op.AddLogFields(logging.Fields{"user": user.Username})

	if perms == nil {
		// no special permissions required, no need to authorize (used for delete-objects, where permissions are checked separately)
		return op
	}
	// authorize
	authResp, err := s.authService.Authorize(&auth.AuthorizationRequest{
		Username:            op.Principal,
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

func authenticate(authenticator sig.SigAuthenticator, authContext sig.SigContext, o *operations.Operation, s *ServerContext) *model.User {
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
	return user
}

func operation(sc *ServerContext, writer http.ResponseWriter, request *http.Request) *operations.Operation {
	return &operations.Operation{
		Request:           request,
		ResponseWriter:    writer,
		Region:            sc.region,
		FQDN:              sc.bareDomain,
		Cataloger:         sc.cataloger,
		MultipartsTracker: sc.multipartsTracker,
		BlockStore:        sc.blockStore,
		Auth:              sc.authService,
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

func RepoOperationHandler(authContext sig.SigContext, s *ServerContext, user *model.User, repo *catalog.Repository) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		operationHandler := repositoryBasedHandler(request.Method)
		perms, err := operationHandler.RequiredPermissions(request, repo.Name)
		o := operation(s, writer, request)
		if err != nil {
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authorize(authContext, s.WithContext(request.Context()), writer, request, user, perms)
		if authOp == nil {
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
		operationHandler.Handle(repoOperation)
	})
}

func PathOperationHandler(authContext sig.SigContext, s *ServerContext, user *model.User, repo *catalog.Repository, refID, path string) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		operationHandler := pathOperationHandler(request.Method)
		// structure operation
		perms, err := operationHandler.RequiredPermissions(request, repo.Name, refID, path)
		o := operation(s, writer, request)
		if err != nil {
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authorize(authContext, s, writer, request, user, perms)
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
		operationHandler.Handle(operation)
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

func notFound(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}

var commaSeparator = regexp.MustCompile(`,\s*`)

var (
	contentTypeApplicationXML = "application/xml"
	contentTypeTextXML        = "text/xml"
)

func selectContentType(acceptable []string) *string {
	for _, acceptableTypes := range acceptable {
		acceptable := commaSeparator.Split(acceptableTypes, -1)
		for _, a := range acceptable {
			switch a {
			case contentTypeTextXML:
				return &contentTypeTextXML
			case contentTypeApplicationXML:
				return &contentTypeApplicationXML
			}
		}
	}
	return nil
}

func setDefaultContentType(w http.ResponseWriter, r *http.Request) {
	acceptable, ok := r.Header["Accept"]
	if ok {
		defaultContentType := selectContentType(acceptable)
		if defaultContentType != nil {
			w.Header().Set("Content-Type", *defaultContentType)
		}
		// If no requested content type matched, still OK at least for proxied content
		// (GET or HEAD), so set up to auto-detect.
	} else {
		w.Header().Set("Content-Type", contentTypeApplicationXML)
		// For proxied content (GET or HEAD) the type will be reset according to
		// whatever headers arrive, including setting up to auto-detect content-type if
		// none is specified by the adapter.
	}
}

func (h *handler) getRepoIDFromAction(r *http.Request) string {
	if strings.EqualFold(httputil.HostOnly(r.Host), httputil.HostOnly(h.BareDomain)) {
		// path style request
		urlPath := strings.TrimPrefix(r.URL.Path, path.Separator)
		if !strings.Contains(urlPath, path.Separator) {
			return urlPath
		}
		return urlPath[:strings.Index(urlPath, path.Separator)]
	}
	// virtual host style:
	host := httputil.HostOnly(r.Host)
	ourHost := httputil.HostOnly(h.BareDomain)
	if !strings.HasSuffix(host, ourHost) {
		return ""
	}
	return strings.TrimSuffix(host, "."+ourHost)

}
func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	o := operation(h.sc, w, req)
	authenticator := sig.ChainedAuthenticator(
		sig.NewV4Authenticator(req),
		sig.NewV2SigAuthenticator(req))
	authContext, err := authenticator.Parse()
	if err != nil {
		o.Log().WithError(err).Warn("failed to parse signature")
		o.EncodeError(getAPIErrOrDefault(err, gatewayerrors.ErrAccessDenied))
		return
	}
	user := authenticate(authenticator, authContext, o, h.sc)
	if user == nil {
		return
	}
	repoID := h.getRepoIDFromAction(req)
	start := time.Now()
	mrw := httputil.NewMetricResponseWriter(w)
	setDefaultContentType(mrw, req)
	h.getHandler(repoID, authContext, user, req).ServeHTTP(mrw, req)
	requestHistograms.WithLabelValues(h.operationID, strconv.Itoa(mrw.StatusCode)).Observe(time.Since(start).Seconds())
}

func (h *handler) getHandler(repoID string, authContext sig.SigContext, user *model.User, req *http.Request) http.Handler {
	if repoID == "" {
		if req.Method == http.MethodGet {
			return OperationHandler(authContext, h.sc, user, &operations.ListBuckets{})
		}
		return unsupportedOperationHandler()
	}
	repo, err := h.sc.cataloger.GetRepository(h.sc.ctx, repoID)
	if errors.Is(err, db.ErrNotFound) {
		listReposPermissions := []permissions.Permission{{
			Action:   permissions.ListRepositoriesAction,
			Resource: "*",
		}}
		if authorize(authContext, h.sc, user, listReposPermissions) != nil {
			if h.sc.fallbackProxy != nil {
				return h.sc.fallbackProxy
			}
			return nil
		}
		o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
		return nil
	}
	if repo == nil {
		o.EncodeError(gatewayerrors.ErrInternalError.ToAPIErr())
		return nil
	}
	ref, pth := h.parts(req)
	if pth != "" {
		return PathOperationHandler(authContext, h.sc, user, repo, ref, pth)
	}
	if ref == "" {
		return RepoOperationHandler(authContext, h.sc, user, repo)
	}
	return h.NotFoundHandler
}

func OperationHandler(authContext sig.SigContext, s *ServerContext, user *model.User, handler operations.AuthenticatedOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		perms, err := handler.RequiredPermissions(request)
		o := operation(s, writer, request)
		if err != nil {
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authorize(authContext, s, writer, request, user, perms)
		if authOp == nil {
			return
		}
		// run callback
		handler.Handle(authOp)
	})
}

func (h *handler) parts(r *http.Request) (ref string, pth string) {
	urlPath := strings.TrimPrefix(r.URL.Path, path.Separator)
	if strings.EqualFold(httputil.HostOnly(r.Host), httputil.HostOnly(h.BareDomain)) {
		// path style request - need to remove repo from path
		if !strings.Contains(urlPath, path.Separator) {
			return "", ""
		}
		urlPath = urlPath[strings.Index(urlPath, path.Separator)+1:]
	}
	parts := strings.SplitN(urlPath, path.Separator, 2)
	if len(parts) == 0 {
		return "", ""
	}
	if len(parts) == 1 {
		return parts[0], ""
	}
	return parts[0], parts[1]
}

func pathOperationHandler(method string) operations.PathOperationHandler {
	switch method {
	case http.MethodDelete:
		return &operations.DeleteObject{}
	case http.MethodPost:
		return &operations.PostObject{}
	case http.MethodGet:
		return &operations.GetObject{}
	case http.MethodHead:
		return &operations.HeadObject{}
	case http.MethodPut:
		return &operations.PutObject{}
	}
	return nil
}

func repositoryBasedHandler(method string) operations.RepoOperationHandler {
	switch method {
	case http.MethodHead:
		return &operations.HeadBucket{}
	case http.MethodPost:
		return &operations.DeleteObjects{}
	case http.MethodGet:
		return &operations.ListObjects{}
	}
	return nil
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
