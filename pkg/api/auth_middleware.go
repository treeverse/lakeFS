package api

import (
	"errors"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/gorilla/sessions"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/authhttp"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/logging"
)

type OIDCConfig = auth.OIDCConfig

type CookieAuthConfig = auth.CookieAuthConfig

// extractSecurityRequirements using Swagger returns an array of security requirements set for the request.
func extractSecurityRequirements(router routers.Router, r *http.Request) (openapi3.SecurityRequirements, error) {
	// Find route
	route, _, err := router.FindRoute(r)
	if err != nil {
		return nil, err
	}
	if route.Operation.Security == nil {
		return route.Spec.Security, nil
	}
	return *route.Operation.Security, nil
}

func GenericAuthMiddleware(logger logging.Logger, authenticator auth.Authenticator, authService auth.Service, oidcConfig *OIDCConfig, cookieAuthConfig *CookieAuthConfig) (func(next http.Handler) http.Handler, error) {
	swagger, err := apigen.GetSwagger()
	if err != nil {
		return nil, err
	}
	sessionStore := sessions.NewCookieStore(authService.SecretStore().SharedSecret())
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			user, err := authhttp.CheckSecurityRequirements(r, swagger.Security, logger, authenticator, authService, sessionStore, oidcConfig, cookieAuthConfig)
			if err != nil {
				writeAuthError(w, r, err, http.StatusUnauthorized, ErrAuthenticatingRequest.Error())
				return
			}
			if user != nil {
				ctx := logging.AddFields(r.Context(), logging.Fields{logging.UserFieldKey: user.Username})
				r = r.WithContext(auth.WithUser(ctx, user))
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}

func AuthMiddleware(logger logging.Logger, swagger *openapi3.T, authenticator auth.Authenticator, authService auth.Service, sessionStore sessions.Store, oidcConfig *OIDCConfig, cookieAuthConfig *CookieAuthConfig) func(next http.Handler) http.Handler {
	router, err := legacy.NewRouter(swagger)
	if err != nil {
		panic(err)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// if request already authenticated
			if _, userNotFoundErr := auth.GetUser(r.Context()); userNotFoundErr == nil {
				next.ServeHTTP(w, r)
				return
			}
			securityRequirements, err := extractSecurityRequirements(router, r)
			if err != nil {
				writeAuthError(w, r, err, http.StatusBadRequest, err.Error())
				return
			}
			user, err := authhttp.CheckSecurityRequirements(r, securityRequirements, logger, authenticator, authService, sessionStore, oidcConfig, cookieAuthConfig)
			if err != nil {
				writeAuthError(w, r, err, http.StatusUnauthorized, ErrAuthenticatingRequest.Error())
				return
			}
			if user != nil {
				ctx := logging.AddFields(r.Context(), logging.Fields{logging.UserFieldKey: user.Username})
				r = r.WithContext(auth.WithUser(ctx, user))
			}
			next.ServeHTTP(w, r)
		})
	}
}

// writeAuthError centralizes error handling logic and avoids duplication
func writeAuthError(w http.ResponseWriter, r *http.Request, err error, defaultStatus int, defaultMsg string) {
	logging.FromContext(r.Context()).WithError(err).Warn("authentication error")
	// Only internal server errors are returned to the client to allow retries.
	// Other errors are masked to avoid exposing sensitive information.
	if errors.Is(err, auth.ErrInternalServerError) {
		writeError(w, r, http.StatusInternalServerError, auth.ErrInternalServerError)
	} else {
		writeError(w, r, defaultStatus, defaultMsg)
	}
}
