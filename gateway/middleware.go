package gateway

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/treeverse/lakefs/catalog"

	"github.com/treeverse/lakefs/gateway/simulator"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/db"
	gatewayerrors "github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/operations"
	"github.com/treeverse/lakefs/gateway/path"
	"github.com/treeverse/lakefs/gateway/sig"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/permissions"
)

func AuthenticationHandler(authService simulator.GatewayAuthService, bareDomain string, next http.Handler) http.Handler {
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
		creds, err := authService.GetCredentials(authContext.GetAccessKeyID())
		if err != nil {
			if !errors.Is(err, db.ErrNotFound) {
				o.Log(r).WithError(err).WithField("key", authContext.GetAccessKeyID()).Warn("error getting access key")
				o.EncodeError(writer, r, gatewayerrors.ErrInternalError.ToAPIErr())
			} else {
				o.Log(r).WithError(err).WithField("key", authContext.GetAccessKeyID()).Warn("could not find access key")
				o.EncodeError(writer, r, gatewayerrors.ErrAccessDenied.ToAPIErr())
			}
			return
		}
		err = authenticator.Verify(creds, bareDomain)
		if err != nil {
			o.Log(r).WithError(err).WithFields(logging.Fields{
				"key":           authContext.GetAccessKeyID(),
				"authenticator": authenticator,
			}).Warn("error verifying credentials for key")
			o.EncodeError(writer, r, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		user, err := authService.GetUserByID(creds.UserID)
		if err != nil {
			o.Log(r).WithError(err).WithFields(logging.Fields{
				"key":           authContext.GetAccessKeyID(),
				"authenticator": authenticator,
			}).Warn("could not get user for credentials key")
			o.EncodeError(writer, r, gatewayerrors.ErrAccessDenied.ToAPIErr())
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), "user", user))
		r = r.WithContext(context.WithValue(r.Context(), "auth_context", authContext))
		next.ServeHTTP(writer, r)
	})
}

func RepoIDHandler(bareDomain string, next http.Handler) http.Handler {
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
		req = req.WithContext(context.WithValue(req.Context(), "repo_id", repo))
		next.ServeHTTP(w, req)
	})
}
func EnrichOperationHandler(sc *ServerContext, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		o := operation(sc, req.Context())
		next.ServeHTTP(w, req.WithContext(context.WithValue(req.Context(), "operation", o)))
	})
}
func DurationHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		o := req.Context().Value("operation").(*operations.Operation)
		start := time.Now()
		mrw := httputil.NewMetricResponseWriter(w)
		next.ServeHTTP(w, req)
		requestHistograms.WithLabelValues(o.OperationID, strconv.Itoa(mrw.StatusCode)).Observe(time.Since(start).Seconds())
	})
}

// TODO split fallback to handler
func EnrichRepoHandler(cataloger catalog.Cataloger, authService simulator.GatewayAuthService, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		repoID := r.Context().Value("repo_id").(string)
		username := r.Context().Value("user").(*model.User).Username
		o := r.Context().Value("operation").(*operations.Operation)
		if repoID == "" {
			// action without repo
			next.ServeHTTP(writer, r)
			return
		}
		repo, err := cataloger.GetRepository(r.Context(), repoID)
		if errors.Is(err, db.ErrNotFound) {
			authResp, authErr := authService.Authorize(&auth.AuthorizationRequest{
				Username:            username,
				RequiredPermissions: []permissions.Permission{{Action: permissions.ListRepositoriesAction, Resource: "*"}},
			})
			if authErr != nil || authResp.Error != nil || !authResp.Allowed {
				o.EncodeError(writer, r, gatewayerrors.ErrAccessDenied.ToAPIErr())
			}
			//if h.sc.fallbackProxy != nil {
			//	h.sc.fallbackProxy.ServeHTTP(writer, r)
			//}
			o.EncodeError(writer, r, gatewayerrors.ErrNoSuchBucket.ToAPIErr())
			return
		}
		if repo == nil {
			o.EncodeError(writer, r, gatewayerrors.ErrInternalError.ToAPIErr())
			return
		}
		r = r.WithContext(context.WithValue(r.Context(), "repo", repo))
		next.ServeHTTP(writer, r)
	})
}

func parts(r *http.Request, bareDomain string) (ref *string, pth *string) {
	urlPath := strings.TrimPrefix(r.URL.Path, path.Separator)
	if strings.EqualFold(httputil.HostOnly(r.Host), httputil.HostOnly(bareDomain)) {
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

func OperationLookupHandler(bareDomain string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		o := r.Context().Value("operation").(*operations.Operation)
		repoID := r.Context().Value("repo_id").(string)
		var operationID string
		if repoID == "" {
			if r.Method == http.MethodGet {
				operationID = operations.OperationIDListBuckets
			} else {
				o.EncodeError(writer, r, gatewayerrors.ERRLakeFSNotSupported.ToAPIErr())
			}
		} else {
			ref, pth := parts(r, bareDomain)
			if ref != nil && pth != nil {
				r = r.WithContext(context.WithValue(r.Context(), "ref", *ref))
				r = r.WithContext(context.WithValue(r.Context(), "path", *pth))
				operationID = pathBasedOperationID(r.Method)
			} else if ref != nil && *ref != "" && pth == nil {
				writer.WriteHeader(http.StatusNotFound)
			} else {
				operationID = repositoryBasedOperationID(r.Method)
			}
		}
		o.OperationID = operationID
		next.ServeHTTP(writer, r)
	})
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
