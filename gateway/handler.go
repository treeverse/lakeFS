package gateway

import (
	"context"
	"net/http"
	gohttputil "net/http/httputil"
	"net/url"
	"regexp"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/dedup"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/multiparts"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/sig"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/stats"
)

type ContextKey string

const (
	ContextKeyUser            ContextKey = "user"
	ContextKeyRepositoryID    ContextKey = "repository_id"
	ContextKeyRepository      ContextKey = "repository"
	ContextKeyAuthContext     ContextKey = "auth_context"
	ContextKeyOperation       ContextKey = "operation"
	ContextKeyRef             ContextKey = "ref"
	ContextKeyPath            ContextKey = "path"
	ContextKeyOriginalRequest ContextKey = "original_request"
)

var commaSeparator = regexp.MustCompile(`,\s*`)

var (
	contentTypeApplicationXML = "application/xml"
	contentTypeTextXML        = "text/xml"
)

type handler struct {
	BareDomain         string
	sc                 *ServerContext
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
		ServerErrorHandler: nil,
	}
	h = simulator.RegisterRecorder(httputil.LoggingMiddleware(
		"X-Amz-Request-Id", logging.Fields{"service_name": "s3_gateway"}, h,
	), authService, region, bareDomain)
	h = EnrichOriginalRequest(
		EnrichOperationHandler(sc,
			DurationHandler(
				AuthenticationHandler(authService, bareDomain,
					RepoIDHandler(bareDomain,
						OperationLookupHandler(bareDomain,
							EnrichRepoHandler(cataloger, authService, fallbackProxy,
								h)))))))
	logging.Default().WithFields(logging.Fields{
		"s3_bare_domain": bareDomain,
		"s3_region":      region,
	}).Info("initialized S3 Gateway handler")
	return h
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	o := req.Context().Value(ContextKeyOperation).(*operations.Operation)
	setDefaultContentType(w, req)
	actionHandler := getOperationHandler(h.sc, o.OperationID)
	if actionHandler == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	actionHandler.ServeHTTP(w, req)
}

func getAPIErrOrDefault(err error, defaultAPIErr gatewayerrors.APIErrorCode) gatewayerrors.APIError {
	apiError, ok := err.(*gatewayerrors.APIErrorCode)
	if ok {
		return apiError.ToAPIErr()
	} else {
		return defaultAPIErr.ToAPIErr()
	}
}

func OperationHandler(sc *ServerContext, handler operations.AuthenticatedOperationHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		username := req.Context().Value(ContextKeyUser).(*model.User).Username
		authContext := req.Context().Value(ContextKeyAuthContext).(sig.SigContext)
		o := req.Context().Value(ContextKeyOperation).(*operations.Operation)
		perms, err := handler.RequiredPermissions(req)
		if err != nil {
			_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := &operations.AuthenticatedOperation{
			Operation: o,
			Principal: username,
		}
		if !authorize(w, req, authOp, authContext, sc.authService, perms) {
			return
		}
		handler.Handle(w, req, authOp)
	})
}

func RepoOperationHandler(sc *ServerContext, handler operations.RepoOperationHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		username := req.Context().Value(ContextKeyUser).(*model.User).Username
		repo := req.Context().Value(ContextKeyRepository).(*catalog.Repository)
		authContext := req.Context().Value(ContextKeyAuthContext).(sig.SigContext)
		o := req.Context().Value(ContextKeyOperation).(*operations.Operation)
		perms, err := handler.RequiredPermissions(req, repo.Name)
		if err != nil {
			_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := &operations.AuthenticatedOperation{
			Operation: o,
			Principal: username,
		}
		if !authorize(w, req, authOp, authContext, sc.authService, perms) {
			return
		}
		repoOperation := &operations.RepoOperation{
			AuthenticatedOperation: authOp,
			Repository:             repo,
		}
		logging.AddFields(req.Context(), logging.Fields{
			"repository": repo.Name,
		})
		handler.Handle(w, req, repoOperation)
	})
}

func PathOperationHandler(sc *ServerContext, handler operations.PathOperationHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		username := req.Context().Value(ContextKeyUser).(*model.User).Username
		repo := req.Context().Value(ContextKeyRepository).(*catalog.Repository)
		refID := req.Context().Value(ContextKeyRef).(string)
		path := req.Context().Value(ContextKeyPath).(string)
		authContext := req.Context().Value(ContextKeyAuthContext).(sig.SigContext)
		o := req.Context().Value(ContextKeyOperation).(*operations.Operation)
		perms, err := handler.RequiredPermissions(req, repo.Name, refID, path)
		if err != nil {
			_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := &operations.AuthenticatedOperation{
			Operation: o,
			Principal: username,
		}
		if !authorize(w, req, authOp, authContext, sc.authService, perms) {
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
		logging.AddFields(req.Context(), logging.Fields{
			"repository": repo.Name,
			"ref":        refID,
			"path":       path,
		})
		handler.Handle(w, req, operation)
	})
}

func authorize(w http.ResponseWriter, req *http.Request, o *operations.AuthenticatedOperation, authContext sig.SigContext, authService simulator.GatewayAuthService, perms []permissions.Permission) bool {
	authResp, err := authService.Authorize(&auth.AuthorizationRequest{
		Username:            o.Principal,
		RequiredPermissions: perms,
	})
	if err != nil {
		o.Log(req).WithError(err).Error("failed to authorize")
		_ = o.EncodeError(w, req, gatewayerrors.ErrInternalError.ToAPIErr())
		return false
	}
	if authResp.Error != nil || !authResp.Allowed {
		o.Log(req).WithError(authResp.Error).WithField("key", authContext.GetAccessKeyID()).Warn("no permission")
		_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
		return false
	}
	return true
}

func operation(sc *ServerContext, ctx context.Context) *operations.Operation {
	return &operations.Operation{
		Region:            sc.region,
		FQDN:              sc.bareDomain,
		Cataloger:         sc.cataloger,
		MultipartsTracker: sc.multipartsTracker,
		BlockStore:        sc.blockStore,
		Auth:              sc.authService,
		Incr: func(action string) {
			logging.FromContext(ctx).
				WithField("action", action).
				WithField("message_type", "action").
				Debug("performing S3 action")
			sc.stats.CollectEvent("s3_gateway", action)
		},
	}
}

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

func getOperationHandler(sc *ServerContext, operationID string) http.Handler {
	switch operationID {
	case operations.OperationIDDeleteObject:
		return PathOperationHandler(sc, &operations.DeleteObject{})
	case operations.OperationIDDeleteObjects:
		return RepoOperationHandler(sc, &operations.DeleteObjects{})
	case operations.OperationIDGetObject:
		return PathOperationHandler(sc, &operations.GetObject{})
	case operations.OperationIDHeadBucket:
		return RepoOperationHandler(sc, &operations.HeadBucket{})
	case operations.OperationIDHeadObject:
		return PathOperationHandler(sc, &operations.HeadObject{})
	case operations.OperationIDListBuckets:
		return OperationHandler(sc, &operations.ListBuckets{})
	case operations.OperationIDListObjects:
		return RepoOperationHandler(sc, &operations.ListObjects{})
	case operations.OperationIDPostObject:
		return PathOperationHandler(sc, &operations.PostObject{})
	case operations.OperationIDPutObject:
		return PathOperationHandler(sc, &operations.PutObject{})
	case operations.OperationIDUnsupportedOperation:
		return unsupportedOperationHandler()
	default:
		return nil
	}
}

func unsupportedOperationHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		o := &operations.Operation{}
		_ = o.EncodeError(w, req, gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
	})
}
