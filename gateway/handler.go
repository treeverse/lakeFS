package gateway

import (
	"context"
	"net/http"
	gohttputil "net/http/httputil"
	"net/url"
	"regexp"

	"github.com/treeverse/lakefs/auth/model"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/gateway/sig"
	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/dedup"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/multiparts"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/stats"
)

type handler struct {
	BareDomain         string
	sc                 *ServerContext
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
	h = EnrichOperationHandler(sc,
		DurationHandler(
			AuthenticationHandler(authService, bareDomain,
				RepoIDHandler(bareDomain,
					OperationLookupHandler(bareDomain,
						EnrichRepoHandler(cataloger, authService, h))))))
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

func RepoOperationHandler(sc *ServerContext, handler operations.RepoOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		username := request.Context().Value("user").(*model.User).Username
		repoID := request.Context().Value("repo_id").(string)
		repo := request.Context().Value("repo").(*catalog.Repository)
		authContext := request.Context().Value("auth_context").(sig.SigContext)
		o := request.Context().Value("operation").(*operations.Operation)
		perms, err := handler.RequiredPermissions(request, repoID)
		if err != nil {
			o.EncodeError(writer, request, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := &operations.AuthenticatedOperation{
			Operation: o,
			Principal: username,
		}
		if !authorize(writer, request, authOp, authContext, sc.authService, perms) {
			return
		}
		repoOperation := &operations.RepoOperation{
			AuthenticatedOperation: authOp,
			Repository:             repo,
		}
		repoOperation.AddLogFields(request, logging.Fields{
			"repository": repo.Name,
		})
		handler.Handle(writer, request, repoOperation)
	})
}

func authorize(w http.ResponseWriter, r *http.Request, o *operations.AuthenticatedOperation, authContext sig.SigContext, authService simulator.GatewayAuthService, perms []permissions.Permission) bool {
	authResp, err := authService.Authorize(&auth.AuthorizationRequest{
		Username:            o.Principal,
		RequiredPermissions: perms,
	})
	if err != nil {
		o.Log(r).WithError(err).Error("failed to authorize")
		o.EncodeError(w, r, gatewayerrors.ErrInternalError.ToAPIErr())
		return false
	}
	if authResp.Error != nil || !authResp.Allowed {
		o.Log(r).WithError(authResp.Error).WithField("key", authContext.GetAccessKeyID()).Warn("no permission")
		o.EncodeError(w, r, gatewayerrors.ErrAccessDenied.ToAPIErr())
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
		DedupCleaner: sc.dedupCleaner,
	}
}

func PathOperationHandler(sc *ServerContext, handler operations.PathOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		username := request.Context().Value("user").(*model.User).Username
		repoID := request.Context().Value("repo_id").(string)
		repo := request.Context().Value("repo").(*catalog.Repository)
		refID := request.Context().Value("ref").(string)
		path := request.Context().Value("path").(string)
		authContext := request.Context().Value("auth_context").(sig.SigContext)
		o := request.Context().Value("operation").(*operations.Operation)
		perms, err := handler.RequiredPermissions(request, repoID, refID, path)
		if err != nil {
			o.EncodeError(writer, request, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := &operations.AuthenticatedOperation{
			Operation: o,
			Principal: username,
		}
		if !authorize(writer, request, authOp, authContext, sc.authService, perms) {
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
		operation.AddLogFields(request, logging.Fields{
			"repository": repo.Name,
			"ref":        refID,
			"path":       path,
		})
		handler.Handle(writer, request, operation)
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

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	o := req.Context().Value("operation").(*operations.Operation)
	setDefaultContentType(w, req)
	actionHandler := getOperationHandler(h.sc, o.OperationID)
	if actionHandler == nil {
		h.NotFoundHandler.ServeHTTP(w, req)
		return
	}
	actionHandler.ServeHTTP(w, req)
}

func OperationHandler(sc *ServerContext, handler operations.AuthenticatedOperationHandler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		username := request.Context().Value("user").(*model.User).Username
		authContext := request.Context().Value("auth_context").(sig.SigContext)
		o := request.Context().Value("operation").(*operations.Operation)
		perms, err := handler.RequiredPermissions(request)
		if err != nil {
			o.EncodeError(writer, request, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		authOp := &operations.AuthenticatedOperation{
			Operation: o,
			Principal: username,
		}
		if !authorize(writer, request, authOp, authContext, sc.authService, perms) {
			return
		}
		handler.Handle(writer, request, authOp)
	})
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
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		o := &operations.Operation{}
		o.EncodeError(writer, request, gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
	})
}
