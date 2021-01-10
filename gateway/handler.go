package gateway

import (
	"context"
	"net/http"
	gohttputil "net/http/httputil"
	"net/url"
	"regexp"
	"strings"

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

type contextKey string

const (
	ContextKeyUser         contextKey = "user"
	ContextKeyRepositoryID contextKey = "repository_id"
	ContextKeyRepository   contextKey = "repository"
	ContextKeyAuthContext  contextKey = "auth_context"
	ContextKeyOperation    contextKey = "operation"
	ContextKeyRef          contextKey = "ref"
	ContextKeyPath         contextKey = "path"
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
	operationHandlers  map[operations.OperationID]http.Handler
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
	var fallbackHandler http.Handler
	if fallbackURL != nil {
		fallbackProxy := gohttputil.NewSingleHostReverseProxy(fallbackURL)
		fallbackHandler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			request.Host = strings.Replace(request.Host, bareDomain, fallbackURL.Host, 1)
			fallbackProxy.ServeHTTP(writer, request)
		})
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
	}

	// setup routes
	var h http.Handler
	h = &handler{
		BareDomain:         bareDomain,
		sc:                 sc,
		ServerErrorHandler: nil,
		operationHandlers: map[operations.OperationID]http.Handler{
			operations.OperationIDDeleteObject:         PathOperationHandler(sc, &operations.DeleteObject{}),
			operations.OperationIDDeleteObjects:        RepoOperationHandler(sc, &operations.DeleteObjects{}),
			operations.OperationIDGetObject:            PathOperationHandler(sc, &operations.GetObject{}),
			operations.OperationIDHeadBucket:           RepoOperationHandler(sc, &operations.HeadBucket{}),
			operations.OperationIDHeadObject:           PathOperationHandler(sc, &operations.HeadObject{}),
			operations.OperationIDListBuckets:          OperationHandler(sc, &operations.ListBuckets{}),
			operations.OperationIDListObjects:          RepoOperationHandler(sc, &operations.ListObjects{}),
			operations.OperationIDPostObject:           PathOperationHandler(sc, &operations.PostObject{}),
			operations.OperationIDPutObject:            PathOperationHandler(sc, &operations.PutObject{}),
			operations.OperationIDUnsupportedOperation: unsupportedOperationHandler(),
		},
	}
	h = simulator.RegisterRecorder(httputil.LoggingMiddleware(
		"X-Amz-Request-Id", logging.Fields{"service_name": "s3_gateway"}, h,
	), authService, region, bareDomain)
	h = EnrichWithOperation(sc,
		DurationHandler(
			AuthenticationHandler(authService, bareDomain,
				EnrichWithParts(bareDomain,
					EnrichWithRepositoryOrFallback(cataloger, authService, fallbackHandler,
						OperationLookupHandler(
							h))))))
	logging.Default().WithFields(logging.Fields{
		"s3_bare_domain": bareDomain,
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
		}
		req = req.WithContext(logging.AddFields(ctx, logging.Fields{
			"repository": repo.Name,
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
		o := ctx.Value(ContextKeyOperation).(*operations.Operation)
		perms, err := handler.RequiredPermissions(req, repo.Name, refID, path)
		if err != nil {
			_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
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
				},
				Reference: refID,
			},
			Path: path,
		}
		req = req.WithContext(logging.AddFields(ctx, logging.Fields{
			"repository": repo.Name,
			"ref":        refID,
			"path":       path,
		}))
		handler.Handle(w, req, operation)
	})
}

func authorize(w http.ResponseWriter, req *http.Request, authService simulator.GatewayAuthService, perms []permissions.Permission) *operations.AuthorizedOperation {
	ctx := req.Context()
	o := ctx.Value(ContextKeyOperation).(*operations.Operation)
	username := ctx.Value(ContextKeyUser).(*model.User).Username
	authContext := ctx.Value(ContextKeyAuthContext).(sig.SigContext)
	authResp, err := authService.Authorize(&auth.AuthorizationRequest{
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

func unsupportedOperationHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		o := &operations.Operation{}
		_ = o.EncodeError(w, req, gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
	})
}
