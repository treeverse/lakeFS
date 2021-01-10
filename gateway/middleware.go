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
		o := req.Context().Value(ContextKeyOperation).(*operations.Operation)
		authenticator := sig.ChainedAuthenticator(
			sig.NewV4Authenticator(req),
			sig.NewV2SigAuthenticator(req))
		authContext, err := authenticator.Parse()
		if err != nil {
			o.Log(req).WithError(err).Warn("failed to parse signature")
			_ = o.EncodeError(w, req, getAPIErrOrDefault(err, gatewayerrors.ErrAccessDenied))
			return
		}
		creds, err := authService.GetCredentials(authContext.GetAccessKeyID())
		if err != nil {
			if !errors.Is(err, db.ErrNotFound) {
				o.Log(req).WithError(err).WithField("key", authContext.GetAccessKeyID()).Warn("error getting access key")
				_ = o.EncodeError(w, req, gatewayerrors.ErrInternalError.ToAPIErr())
			} else {
				o.Log(req).WithError(err).WithField("key", authContext.GetAccessKeyID()).Warn("could not find access key")
				_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			}
			return
		}
		err = authenticator.Verify(creds, bareDomain)
		if err != nil {
			o.Log(req).WithError(err).WithFields(logging.Fields{
				"key":           authContext.GetAccessKeyID(),
				"authenticator": authenticator,
			}).Warn("error verifying credentials for key")
			_ = o.EncodeError(w, req, getAPIErrOrDefault(err, gatewayerrors.ErrAccessDenied))
			return
		}
		user, err := authService.GetUserByID(creds.UserID)
		if err != nil {
			o.Log(req).WithError(err).WithFields(logging.Fields{
				"key":           authContext.GetAccessKeyID(),
				"authenticator": authenticator,
			}).Warn("could not get user for credentials key")
			_ = o.EncodeError(w, req, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		req = req.WithContext(logging.AddFields(req.Context(), logging.Fields{"user": user.Username}))
		req = req.WithContext(context.WithValue(req.Context(), ContextKeyUser, user))
		req = req.WithContext(context.WithValue(req.Context(), ContextKeyAuthContext, authContext))
		next.ServeHTTP(w, req)
	})
}

func EnrichWithRepoID(bareDomain string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var repo string
		if strings.EqualFold(httputil.HostOnly(req.Host), httputil.HostOnly(bareDomain)) {
			// path style request
			urlPath := strings.TrimPrefix(req.URL.Path, path.Separator)
			if !strings.Contains(urlPath, path.Separator) {
				repo = urlPath
			} else {
				repo = urlPath[:strings.Index(urlPath, path.Separator)]
			}
		} else {
			host := httputil.HostOnly(req.Host)
			ourHost := httputil.HostOnly(bareDomain)
			if !strings.HasSuffix(host, ourHost) {
				repo = ""
			} else {
				repo = strings.TrimSuffix(host, "."+ourHost)
			}
		}
		req = req.WithContext(context.WithValue(req.Context(), ContextKeyRepositoryID, repo))
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
		o := operation(sc, req.Context())
		next.ServeHTTP(w, req.WithContext(context.WithValue(req.Context(), ContextKeyOperation, o)))
	})
}

func DurationHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		o := req.Context().Value(ContextKeyOperation).(*operations.Operation)
		start := time.Now()
		mrw := httputil.NewMetricResponseWriter(w)
		next.ServeHTTP(w, req)
		requestHistograms.WithLabelValues(o.OperationID, strconv.Itoa(mrw.StatusCode)).Observe(time.Since(start).Seconds())
	})
}

func EnrichWithRepository(cataloger catalog.Cataloger, authService simulator.GatewayAuthService, fallbackProxy http.Handler, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		repoID := req.Context().Value(ContextKeyRepositoryID).(string)
		username := req.Context().Value(ContextKeyUser).(*model.User).Username
		o := req.Context().Value(ContextKeyOperation).(*operations.Operation)
		if repoID == "" {
			// action without repo
			next.ServeHTTP(w, req)
			return
		}
		repo, err := cataloger.GetRepository(req.Context(), repoID)
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
				originalRequest := req.Context().Value(ContextKeyOriginalRequest).(*http.Request)
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
		req = req.WithContext(context.WithValue(req.Context(), ContextKeyRepository, repo))
		next.ServeHTTP(w, req)
	})
}

func OperationLookupHandler(bareDomain string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		o := req.Context().Value(ContextKeyOperation).(*operations.Operation)
		repoID := req.Context().Value(ContextKeyRepositoryID).(string)
		var operationID string
		if repoID == "" {
			if req.Method == http.MethodGet {
				operationID = operations.OperationIDListBuckets
			} else {
				_ = o.EncodeError(w, req, gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
				return
			}
		} else {
			ref, pth := parts(req, bareDomain)
			switch {
			case ref != nil && pth != nil:
				req = req.WithContext(context.WithValue(req.Context(), ContextKeyRef, *ref))
				req = req.WithContext(context.WithValue(req.Context(), ContextKeyPath, *pth))
				operationID = pathBasedOperationID(req.Method)
			case (ref == nil || *ref == "") && pth == nil:
				operationID = repositoryBasedOperationID(req.Method)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}
		o.OperationID = operationID
		next.ServeHTTP(w, req)
	})
}

func parts(req *http.Request, bareDomain string) (ref *string, pth *string) {
	urlPath := strings.TrimPrefix(req.URL.Path, path.Separator)
	if strings.EqualFold(httputil.HostOnly(req.Host), httputil.HostOnly(bareDomain)) {
		// path style request - need to remove repo from path
		if !strings.Contains(urlPath, path.Separator) {
			return nil, nil
		}
		urlPath = urlPath[strings.Index(urlPath, path.Separator)+1:]
	}
	p := strings.SplitN(urlPath, path.Separator, 2)
	if len(p) == 0 {
		return nil, nil
	}
	if len(p) == 1 {
		return &p[0], nil
	}
	return &p[0], &p[1]
}

func pathBasedOperationID(method string) string {
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

func repositoryBasedOperationID(method string) string {
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
