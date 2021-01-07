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

func (h *handler) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		o := r.Context().Value("operation").(*operations.Operation)
		username := r.Context().Value("user").(*model.User).Username
		authContext := r.Context().Value("authContext").(sig.SigContext)
		op := &operations.AuthenticatedOperation{
			Operation: o,
			Principal: username,
		}
		var perms []permissions.Permission // TODO get perms
		if perms == nil {
			// TODO err
		}
		if len(perms) == 0 {
			// no perms required
			return
		}
		authResp, err := h.sc.authService.Authorize(&auth.AuthorizationRequest{
			Username:            op.Principal,
			RequiredPermissions: perms,
		})
		if err != nil {
			o.Log().WithError(err).Error("failed to authorize")
			o.EncodeError(gatewayerrors.ErrInternalError.ToAPIErr())
			return
		}
		if authResp.Error != nil || !authResp.Allowed {
			o.Log().WithError(authResp.Error).WithField("key", authContext.GetAccessKeyID()).Warn("no permission")
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), "authorized_operation", op))
		next.ServeHTTP(writer, r)
	})
}

func (h *handler) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		o := r.Context().Value("operation").(*operations.Operation)
		authenticator := sig.ChainedAuthenticator(
			sig.NewV4Authenticator(r),
			sig.NewV2SigAuthenticator(r))
		authContext, err := authenticator.Parse()
		if err != nil {
			// TODO err
			return
		}
		creds, err := h.sc.authService.GetCredentials(authContext.GetAccessKeyID())
		if err != nil {
			if !errors.Is(err, db.ErrNotFound) {
				o.Log().WithError(err).WithField("key", authContext.GetAccessKeyID()).Warn("error getting access key")
				o.EncodeError(gatewayerrors.ErrInternalError.ToAPIErr())
			} else {
				o.Log().WithError(err).WithField("key", authContext.GetAccessKeyID()).Warn("could not find access key")
				o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			}
			return
		}
		err = authenticator.Verify(creds, h.sc.bareDomain)
		if err != nil {
			o.Log().WithError(err).WithFields(logging.Fields{
				"key":           authContext.GetAccessKeyID(),
				"authenticator": authenticator,
			}).Warn("error verifying credentials for key")
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		user, err := h.sc.authService.GetUserByID(creds.UserID)
		if err != nil {
			o.Log().WithError(err).WithFields(logging.Fields{
				"key":           authContext.GetAccessKeyID(),
				"authenticator": authenticator,
			}).Warn("could not get user for credentials key")
			o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), "user", user))
		next.ServeHTTP(writer, r)
	})
}

func (h *handler) RepoOperationHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// structure operation
		repo := request.Context().Value("repo").(*catalog.Repository)
		authOp := request.Context().Value("authorized_operation").(*operations.AuthenticatedOperation)
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
		next.ServeHTTP(writer, request)
	})
}

func (h *handler) PathOperationHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		repo := request.Context().Value("repo").(*catalog.Repository)
		authOp := request.Context().Value("authorized_operation").(*operations.AuthenticatedOperation)
		refID := request.Context().Value("ref").(string)
		pth := request.Context().Value("path").(string)
		if authOp == nil {
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
			Path: pth,
		}
		operation.AddLogFields(logging.Fields{
			"repository": repo.Name,
			"ref":        refID,
			"path":       pth,
		})
		next.ServeHTTP(writer, request)
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

func (h *handler) extractRepoID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var repo string
		if strings.EqualFold(httputil.HostOnly(req.Host), httputil.HostOnly(h.BareDomain)) {
			// path style request
			urlPath := strings.TrimPrefix(req.URL.Path, path.Separator)
			if !strings.Contains(urlPath, path.Separator) {
				repo = urlPath
			} else {
				repo = urlPath[:strings.Index(urlPath, path.Separator)]
			}
		} else {
			host := httputil.HostOnly(req.Host)
			ourHost := httputil.HostOnly(h.BareDomain)
			if !strings.HasSuffix(host, ourHost) {
				repo = ""
			} else {
				repo = strings.TrimSuffix(host, "."+ourHost)
			}
		}
		req = req.WithContext(context.WithValue(req.Context(), "repo_id", repo))
		next.ServeHTTP(w, req)
	})
}

func (h *handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	o := &operations.Operation{}
	req = req.WithContext(context.WithValue(req.Context(), "operation", o))
	setDefaultContentType(w, req)
	h.durations(
		h.authenticate(
			h.findOperation(
				h.extractRepoID(
					h.getRepoOrDoFallback(
						h.authorize(nil)))))).ServeHTTP(w, req)

}

func (h *handler) durations(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		o := req.Context().Value("operation").(*operations.Operation)
		start := time.Now()
		mrw := httputil.NewMetricResponseWriter(w)
		next.ServeHTTP(w, req)
		requestHistograms.WithLabelValues(o.OperationID, strconv.Itoa(mrw.StatusCode)).Observe(time.Since(start).Seconds())
	})
}
func (h *handler) getRepoOrDoFallback(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		repoID := r.Context().Value("repo_id").(string)
		username := r.Context().Value("user").(*model.User).Username
		o := r.Context().Value("operation").(*operations.Operation)
		if repoID == "" {
			// action without repo
			next.ServeHTTP(writer, r)
		}
		repo, err := h.sc.cataloger.GetRepository(h.sc.ctx, repoID)
		if errors.Is(err, db.ErrNotFound) {
			authResp, authErr := h.sc.authService.Authorize(&auth.AuthorizationRequest{
				Username:            username,
				RequiredPermissions: []permissions.Permission{{Action: permissions.ListRepositoriesAction, Resource: "*"}},
			})
			if authErr != nil || authResp.Error != nil || !authResp.Allowed {
				o.EncodeError(gatewayerrors.ErrAccessDenied.ToAPIErr())
			}
			if h.sc.fallbackProxy != nil {
				h.sc.fallbackProxy.ServeHTTP(writer, r)
			}
			o.EncodeError(gatewayerrors.ErrNoSuchBucket.ToAPIErr())
			return
		}
		if repo == nil {
			o.EncodeError(gatewayerrors.ErrInternalError.ToAPIErr())
			return
		}
		next.ServeHTTP(writer, r.WithContext(context.WithValue(r.Context(), "repo", repo)))
	})
}

func (h *handler) ListBucketsOperationHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// TODO
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

func (h *handler) findOperation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		o := r.Context().Value("operation").(*operations.Operation)
		repoID := r.Context().Value("repo_id").(string)
		var operationID string
		if repoID == "" {
			if r.Method == http.MethodGet {
				operationID = "list_buckets"
			} else {
				o.EncodeError(gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
			}
		} else {
			ref, pth := h.parts(r)
			if ref == "" && pth == "" {
				// TODO find repo based operation
			} else if ref != "" && pth == "" {
				// TODO err
			} else {
				// TODO find path based operation
			}
			r = r.WithContext(context.WithValue(r.Context(), "ref", ref))
			r = r.WithContext(context.WithValue(r.Context(), "path", pth))
		}
		o.OperationID = operationID
		next.ServeHTTP(writer, r)
	})
}
