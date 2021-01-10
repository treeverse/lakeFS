package gateway

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/sig"
	"github.com/treeverse/lakefs/gateway/simulator"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
)

func AuthenticationHandler(authService simulator.GatewayAuthService, bareDomain string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		o := ctx.Value(ContextKeyOperation).(*operations.Operation)
		authenticator := sig.ChainedAuthenticator(
			sig.NewV4Authenticator(req),
			sig.NewV2SigAuthenticator(req))
		authContext, err := authenticator.Parse()
		if err != nil {
			o.Log(req).WithError(err).Warn("failed to parse signature")
			_ = o.EncodeError(w, req, getAPIErrOrDefault(err, gatewayerrors.ErrAccessDenied))
			return
		}
		accessKeyID := authContext.GetAccessKeyID()
		creds, err := authService.GetCredentials(accessKeyID)
		logger := o.Log(req).WithField("key", accessKeyID)
		if err != nil {
			if !errors.Is(err, db.ErrNotFound) {
				logger.WithError(err).Warn("error getting access key")
				_ = o.EncodeError(w, req, gatewayerrors.ErrInternalError.ToAPIErr())
			} else {
				logger.WithError(err).Warn("could not find access key")
				_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			}
			return
		}
		err = authenticator.Verify(creds, bareDomain)
		logger = logger.WithField("authenticator", authenticator)
		if err != nil {
			logger.WithError(err).Warn("error verifying credentials for key")
			_ = o.EncodeError(w, req, getAPIErrOrDefault(err, gatewayerrors.ErrAccessDenied))
			return
		}
		user, err := authService.GetUserByID(creds.UserID)
		if err != nil {
			logger.WithError(err).Warn("could not get user for credentials key")
			_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		ctx = logging.AddFields(ctx, logging.Fields{"user": user.Username})
		ctx = context.WithValue(ctx, ContextKeyUser, user)
		ctx = context.WithValue(ctx, ContextKeyAuthContext, authContext)
		req = req.WithContext(ctx)
		next.ServeHTTP(w, req)
	})
}

func EnrichWithParts(bareDomain string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		repo, ref, pth := Parts(req.Host, req.URL.Path, bareDomain)
		ctx = context.WithValue(ctx, ContextKeyRepositoryID, repo)
		ctx = context.WithValue(ctx, ContextKeyRef, ref)
		ctx = context.WithValue(ctx, ContextKeyPath, pth)
		req = req.WithContext(ctx)
		next.ServeHTTP(w, req)
	})
}

func EnrichWithOriginalRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		next.ServeHTTP(w, req.WithContext(context.WithValue(req.Context(), ContextKeyOriginalRequest, req)))
	})
}

func EnrichWithOperation(sc *ServerContext, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		o := &operations.Operation{
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
		next.ServeHTTP(w, req.WithContext(context.WithValue(ctx, ContextKeyOperation, o)))
	})
}

func DurationHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		o := ctx.Value(ContextKeyOperation).(*operations.Operation)
		start := time.Now()
		mrw := httputil.NewMetricResponseWriter(w)
		next.ServeHTTP(w, req)
		requestHistograms.WithLabelValues(string(o.OperationID), strconv.Itoa(mrw.StatusCode)).Observe(time.Since(start).Seconds())
	})
}

func EnrichWithRepository(cataloger catalog.Cataloger, authService simulator.GatewayAuthService, fallbackProxy http.Handler, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		repoID := ctx.Value(ContextKeyRepositoryID).(string)
		username := ctx.Value(ContextKeyUser).(*model.User).Username
		o := ctx.Value(ContextKeyOperation).(*operations.Operation)
		if repoID == "" {
			// action without repo
			next.ServeHTTP(w, req)
			return
		}
		repo, err := cataloger.GetRepository(ctx, repoID)
		if errors.Is(err, db.ErrNotFound) {
			authResp, authErr := authService.Authorize(&auth.AuthorizationRequest{
				Username:            username,
				RequiredPermissions: []permissions.Permission{{Action: permissions.ListRepositoriesAction, Resource: "*"}},
			})
			if authErr != nil || authResp.Error != nil || !authResp.Allowed {
				_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
				return
			}
			if fallbackProxy != nil {
				originalRequest := ctx.Value(ContextKeyOriginalRequest).(*http.Request)
				fallbackProxy.ServeHTTP(w, originalRequest)
				return
			}
			_ = o.EncodeError(w, req, gatewayerrors.ErrNoSuchBucket.ToAPIErr())
			return
		}
		if repo == nil {
			_ = o.EncodeError(w, req, gatewayerrors.ErrInternalError.ToAPIErr())
			return
		}
		req = req.WithContext(context.WithValue(ctx, ContextKeyRepository, repo))
		next.ServeHTTP(w, req)
	})
}

func OperationLookupHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		o := ctx.Value(ContextKeyOperation).(*operations.Operation)
		repoID := ctx.Value(ContextKeyRepositoryID).(string)
		var operationID operations.OperationID
		if repoID == "" {
			if req.Method == http.MethodGet {
				operationID = operations.OperationIDListBuckets
			} else {
				_ = o.EncodeError(w, req, gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
				return
			}
		} else {
			ref := ctx.Value(ContextKeyRef).(string)
			pth := ctx.Value(ContextKeyPath).(string)
			switch {
			case ref != "" && pth != "":
				req = req.WithContext(ctx)
				operationID = pathBasedOperationID(req.Method)
			case ref == "" && pth == "":
				operationID = repositoryBasedOperationID(req.Method)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}
		o.OperationID = operationID
		next.ServeHTTP(w, req)
	})
}

// Parts returns the repo id, ref and path according to whether the request is path-style or virtual-host-style.
func Parts(host string, urlPath string, bareDomain string) (repo string, ref string, pth string) {
	urlPath = strings.TrimPrefix(urlPath, path.Separator)
	var p []string
	if strings.EqualFold(httputil.HostOnly(host), httputil.HostOnly(bareDomain)) {
		// path style: extract repo from first part
		p = strings.SplitN(urlPath, path.Separator, 3)
		repo = p[0]
		if len(p) >= 1 {
			p = p[1:]
		}
	} else {
		// virtual host style: extract repo from subdomain
		host := httputil.HostOnly(host)
		ourHost := httputil.HostOnly(bareDomain)
		if !strings.HasSuffix(host, ourHost) {
			repo = ""
		} else {
			repo = strings.TrimSuffix(host, "."+ourHost)
		}
		p = strings.SplitN(urlPath, path.Separator, 2)
	}
	// extract ref and path from remaining parts
	if len(p) == 0 {
		return repo, "", ""
	}
	if len(p) == 1 {
		return repo, p[0], ""
	}
	return repo, p[0], p[1]
}

func pathBasedOperationID(method string) operations.OperationID {
	switch method {
	case http.MethodDelete:
		return operations.OperationIDDeleteObject
	case http.MethodPost:
		return operations.OperationIDPostObject
	case http.MethodGet:
		return operations.OperationIDGetObject
	case http.MethodHead:
		return operations.OperationIDHeadObject
	case http.MethodPut:
		return operations.OperationIDPutObject
	default:
		return operations.OperationIDOperationNotFound
	}
}

func repositoryBasedOperationID(method string) operations.OperationID {
	switch method {
	case http.MethodDelete, http.MethodPut:
		return operations.OperationIDUnsupportedOperation
	case http.MethodHead:
		return operations.OperationIDHeadBucket
	case http.MethodPost:
		return operations.OperationIDDeleteObjects
	case http.MethodGet:
		return operations.OperationIDListObjects
	default:
		return operations.OperationIDOperationNotFound
	}
}
