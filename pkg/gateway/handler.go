package gateway

import (
	"errors"
	"net/http"
	gohttputil "net/http/httputil"
	"net/url"
	"regexp"
	"strings"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/multipart"
	"github.com/treeverse/lakefs/pkg/gateway/operations"
	"github.com/treeverse/lakefs/pkg/gateway/sig"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/permissions"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/upload"
	"golang.org/x/exp/slices"
)

type contextKey string

const (
	ContextKeyUser         contextKey = "user"
	ContextKeyRepositoryID contextKey = "repository_id"
	ContextKeyRepository   contextKey = "repository"
	ContextKeyAuthContext  contextKey = "auth_context"
	ContextKeyOperation    contextKey = "operation"
	ContextKeyRef          contextKey = "ref"
	ContextKeyPath         contextKey = "path"
	ContextKeyMatchedHost  contextKey = "matched_host"
)

var commaSeparator = regexp.MustCompile(`,\s*`)

var (
	contentTypeApplicationXML = "application/xml"
	contentTypeTextXML        = "text/xml"
)

type handler struct {
	sc                 *ServerContext
	ServerErrorHandler http.Handler
	operationHandlers  map[operations.OperationID]http.Handler
}

type ServerContext struct {
	region           string
	bareDomains      []string
	catalog          catalog.Interface
	multipartTracker multipart.Tracker
	blockStore       block.Adapter
	authService      auth.GatewayService
	stats            stats.Collector
	pathProvider     upload.PathProvider
}

func NewHandler(region string, catalog catalog.Interface, multipartTracker multipart.Tracker, blockStore block.Adapter, authService auth.GatewayService, bareDomains []string, stats stats.Collector, pathProvider upload.PathProvider, fallbackURL *url.URL, auditLogLevel string, traceRequestHeaders bool) http.Handler {
	var fallbackHandler http.Handler
	if fallbackURL != nil {
		fallbackProxy := gohttputil.NewSingleHostReverseProxy(fallbackURL)
		fallbackHandler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			for _, bareDomain := range bareDomains {
				fallback := strings.Replace(request.Host, bareDomain, fallbackURL.Host, 1)
				if fallback != request.Host {
					request.Host = fallback
					break
				}
			}
			fallbackProxy.ServeHTTP(writer, request)
		})
	}
	sc := &ServerContext{
		catalog:          catalog,
		multipartTracker: multipartTracker,
		region:           region,
		bareDomains:      bareDomains,
		blockStore:       blockStore,
		authService:      authService,
		stats:            stats,
		pathProvider:     pathProvider,
	}

	// setup routes
	var h http.Handler
	h = &handler{
		sc:                 sc,
		ServerErrorHandler: nil,
		operationHandlers: map[operations.OperationID]http.Handler{
			operations.OperationIDDeleteObject:         PathOperationHandler(sc, &operations.DeleteObject{}),
			operations.OperationIDDeleteObjects:        RepoOperationHandler(sc, &operations.DeleteObjects{}),
			operations.OperationIDGetObject:            PathOperationHandler(sc, &operations.GetObject{}),
			operations.OperationIDPutBucket:            RepoOperationHandler(sc, &operations.PutBucket{}),
			operations.OperationIDHeadBucket:           RepoOperationHandler(sc, &operations.HeadBucket{}),
			operations.OperationIDHeadObject:           PathOperationHandler(sc, &operations.HeadObject{}),
			operations.OperationIDListBuckets:          OperationHandler(sc, &operations.ListBuckets{}),
			operations.OperationIDListObjects:          RepoOperationHandler(sc, &operations.ListObjects{}),
			operations.OperationIDPostObject:           PathOperationHandler(sc, &operations.PostObject{}),
			operations.OperationIDPutObject:            PathOperationHandler(sc, &operations.PutObject{}),
			operations.OperationIDUnsupportedOperation: unsupportedOperationHandler(),
		},
	}
	loggingMiddleware := httputil.LoggingMiddleware(
		"X-Amz-Request-Id",
		logging.Fields{"service_name": "s3_gateway"},
		auditLogLevel,
		traceRequestHeaders)

	h = loggingMiddleware(h)

	h = EnrichWithOperation(sc,
		DurationHandler(
			AuthenticationHandler(authService, EnrichWithParts(bareDomains,
				EnrichWithRepositoryOrFallback(catalog, authService, fallbackHandler,
					OperationLookupHandler(
						h))))))
	logging.Default().WithFields(logging.Fields{
		"s3_bare_domain": bareDomains,
		"s3_region":      region,
	}).Info("initialized S3 Gateway handler")
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	setDefaultContentType(w, req)
	o := req.Context().Value(ContextKeyOperation).(*operations.Operation)
	operationHandler := h.operationHandlers[o.OperationID]
	if operationHandler == nil {
		// TODO(johnnyaug): consider other status code or add text with unknown gateway operation
		w.WriteHeader(http.StatusNotFound)
		return
	}
	operationHandler.ServeHTTP(w, req)
}

func getAPIErrOrDefault(err error, defaultAPIErr gatewayerrors.APIErrorCode) gatewayerrors.APIError {
	apiError, ok := err.(gatewayerrors.APIErrorCode)
	if ok {
		return apiError.ToAPIErr()
	} else {
		return defaultAPIErr.ToAPIErr()
	}
}

func OperationHandler(sc *ServerContext, handler operations.AuthenticatedOperationHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		o := ctx.Value(ContextKeyOperation).(*operations.Operation)
		perms, err := handler.RequiredPermissions(req)
		if err != nil {
			_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authorize(w, req, sc.authService, perms)
		if authOp == nil {
			return
		}
		handler.Handle(w, req, authOp)
	})
}

func RepoOperationHandler(sc *ServerContext, handler operations.RepoOperationHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		repo := ctx.Value(ContextKeyRepository).(*catalog.Repository)
		matchedHost := ctx.Value(ContextKeyMatchedHost).(bool)
		o := ctx.Value(ContextKeyOperation).(*operations.Operation)
		perms, err := handler.RequiredPermissions(req, repo.Name)
		if err != nil {
			_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := authorize(w, req, sc.authService, perms)
		if authOp == nil {
			return
		}
		repoOperation := &operations.RepoOperation{
			AuthorizedOperation: authOp,
			Repository:          repo,
			MatchedHost:         matchedHost,
		}
		req = req.WithContext(logging.AddFields(ctx, logging.Fields{
			logging.RepositoryFieldKey:  repo.Name,
			logging.MatchedHostFieldKey: matchedHost,
		}))
		handler.Handle(w, req, repoOperation)
	})
}

func PathOperationHandler(sc *ServerContext, handler operations.PathOperationHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		repo := ctx.Value(ContextKeyRepository).(*catalog.Repository)
		refID := ctx.Value(ContextKeyRef).(string)
		path := ctx.Value(ContextKeyPath).(string)
		matchedHost := ctx.Value(ContextKeyMatchedHost).(bool)
		o := ctx.Value(ContextKeyOperation).(*operations.Operation)
		perms, err := handler.RequiredPermissions(req, repo.Name, refID, path)
		if err != nil {
			if errors.Is(err, gatewayerrors.ErrInvalidCopySource) {
				_ = o.EncodeError(w, req, gatewayerrors.ErrInvalidCopySource.ToAPIErr())
			} else {
				_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			}
			return
		}

		authOp := authorize(w, req, sc.authService, perms)
		if authOp == nil {
			return
		}

		// run callback
		operation := &operations.PathOperation{
			RefOperation: &operations.RefOperation{
				RepoOperation: &operations.RepoOperation{
					AuthorizedOperation: authOp,
					Repository:          repo,
					MatchedHost:         matchedHost,
				},
				Reference: refID,
			},
			Path: path,
		}
		req = req.WithContext(logging.AddFields(ctx, logging.Fields{
			logging.RepositoryFieldKey:  repo.Name,
			logging.RefHostFieldKey:     refID,
			logging.PathFieldKey:        path,
			logging.MatchedHostFieldKey: matchedHost,
		}))
		handler.Handle(w, req, operation)
	})
}

func authorize(w http.ResponseWriter, req *http.Request, authService auth.GatewayService, perms permissions.Node) *operations.AuthorizedOperation {
	ctx := req.Context()
	o := ctx.Value(ContextKeyOperation).(*operations.Operation)
	username := ctx.Value(ContextKeyUser).(*model.User).Username
	authContext := ctx.Value(ContextKeyAuthContext).(sig.SigContext)

	if len(perms.Nodes) == 0 && len(perms.Permission.Action) == 0 {
		// has not provided required permissions
		return &operations.AuthorizedOperation{
			Operation: o,
			Principal: username,
		}
	}

	authResp, err := authService.Authorize(req.Context(), &auth.AuthorizationRequest{
		Username:            username,
		RequiredPermissions: perms,
	})
	if err != nil {
		o.Log(req).WithError(err).Error("failed to authorize")
		_ = o.EncodeError(w, req, gatewayerrors.ErrInternalError.ToAPIErr())
		return nil
	}
	if authResp.Error != nil || !authResp.Allowed {
		o.Log(req).WithError(authResp.Error).WithField("key", authContext.GetAccessKeyID()).Warn("no permission")
		_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
		return nil
	}
	return &operations.AuthorizedOperation{
		Operation: o,
		Principal: username,
	}
}

func selectContentType(acceptable []string) *string {
	for _, supportedContentType := range []string{contentTypeApplicationXML, contentTypeTextXML} {
		for _, acceptableTypes := range acceptable {
			acceptable := commaSeparator.Split(acceptableTypes, -1)
			if slices.Contains(acceptable, supportedContentType) {
				return &supportedContentType
			}
		}
	}
	return nil
}

func setDefaultContentType(w http.ResponseWriter, req *http.Request) {
	acceptable, ok := req.Header["Accept"]
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

func unsupportedOperationHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		o := &operations.Operation{}
		_ = o.EncodeError(w, req, gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
	})
}
