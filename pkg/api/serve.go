package api

import (
	"errors"
	"io"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/sessions"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/api/apiutil"
	"github.com/treeverse/lakefs/pkg/api/params"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/license"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	LoggerServiceName = "rest_api"

	extensionValidationExcludeBody = "x-validation-exclude-body"
)

func Serve(cfg config.Config, catalog *catalog.Catalog, authenticator auth.Authenticator, authService auth.Service, authenticationService authentication.Service, blockAdapter block.Adapter, metadataManager auth.MetadataManager, migrator Migrator, collector stats.Collector, actions actionsHandler, auditChecker AuditChecker, logger logging.Logger, gatewayDomains []string, snippets []params.CodeSnippet, pathProvider upload.PathProvider, usageReporter stats.UsageReporterOperations, licenseManager license.Manager) *chi.Mux {
	logger.Info("initialize OpenAPI server")
	swagger, err := apigen.GetSwagger()
	if err != nil {
		panic(err)
	}
	sessionStore := sessions.NewCookieStore(authService.SecretStore().SharedSecret())
	oidcConfig := OIDCConfig(cfg.AuthConfig().OIDC)
	cookieAuthConfig := CookieAuthConfig(cfg.AuthConfig().CookieAuthVerification)
	r := chi.NewRouter()
	apiRouter := r.With(
		OapiRequestValidatorWithOptions(swagger, &openapi3filter.Options{
			AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
		}),
		httputil.LoggingMiddleware(
			httputil.RequestIDHeaderName,
			logging.Fields{logging.ServiceNameFieldKey: LoggerServiceName},
			cfg.GetBaseConfig().Logging.AuditLogLevel,
			cfg.GetBaseConfig().Logging.TraceRequestHeaders,
			authService.IsAdvancedAuth()),
		AuthMiddleware(logger, swagger, authenticator, authService, sessionStore, &oidcConfig, &cookieAuthConfig),
		MetricsMiddleware(swagger, requestHistograms, requestCounter),
	)
	controller := NewController(cfg, catalog, authenticator, authService, authenticationService, blockAdapter, metadataManager, migrator, collector, actions, auditChecker, logger, sessionStore, pathProvider, usageReporter, licenseManager)
	apigen.HandlerFromMuxWithBaseURL(controller, apiRouter, apiutil.BaseURL)

	r.Mount("/_health", httputil.ServeHealth())
	r.Mount("/metrics", promhttp.Handler())
	r.Mount("/_pprof/", httputil.ServePPROF("/_pprof/"))
	r.Mount("/openapi.json", http.HandlerFunc(swaggerSpecHandler))
	r.Mount(apiutil.BaseURL, http.HandlerFunc(InvalidAPIEndpointHandler))
	r.Mount("/logout", NewLogoutHandler(sessionStore, logger, cfg.AuthConfig().LogoutRedirectURL))

	// Configuration flag to control if the embedded UI is served
	// or not and assign the correct handler for each case.
	var rootHandler http.Handler
	if cfg.UIConfig().IsUIEnabled() {
		// Handler which serves the embedded UI
		// as well as handles erroneous S3 gateway requests
		// and returns a compatible response
		rootHandler = NewUIHandler(gatewayDomains, snippets)
	} else {
		// Handler which only handles erroneous S3 gateway requests
		// and returns a compatible response
		rootHandler = NewS3GatewayEndpointErrorHandler(gatewayDomains)
	}
	r.Mount("/", rootHandler)

	authenticationService.RegisterAdditionalRoutes(r, sessionStore)

	return r
}

func swaggerSpecHandler(w http.ResponseWriter, _ *http.Request) {
	reader, err := apigen.GetSwaggerSpecReader()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.Copy(w, reader)
}

// OapiRequestValidatorWithOptions Creates middleware to validate request by swagger spec.
// This middleware is good for net/http either since go-chi is 100% compatible with net/http.
// The original implementation can be found at https://github.com/deepmap/oapi-codegen/blob/master/pkg/chi-middleware/oapi_validate.go
// Use our own implementation in order to:
//  1. Use the latest version kin-openapi (can switch back when oapi-codegen will be updated)
//  2. For file upload wanted to skip body validation for two reasons:
//     a. didn't find a way for the validator to accept any file content type
//     b. didn't want the validator to read the complete request body for the specific request
func OapiRequestValidatorWithOptions(swagger *openapi3.T, options *openapi3filter.Options) func(http.Handler) http.Handler {
	router, err := legacy.NewRouter(swagger)
	if err != nil {
		panic(err)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// find route
			route, m, err := router.FindRoute(r)
			if err != nil {
				// We failed to find a matching route for the request.
				writeError(w, r, http.StatusBadRequest, err.Error())
				return
			}

			// include operation id from route in the context for logging
			r = r.WithContext(logging.AddFields(r.Context(), logging.Fields{"operation_id": route.Operation.OperationID}))

			// validate request
			statusCode, err := validateRequest(r, route, m, options)
			if err != nil {
				writeError(w, r, statusCode, err.Error())
				return
			}
			// serve
			next.ServeHTTP(w, r)
		})
	}
}

func validateRequest(r *http.Request, route *routers.Route, pathParams map[string]string, options *openapi3filter.Options) (int, error) {
	// Extension - validation exclude body
	if _, ok := route.Operation.Extensions[extensionValidationExcludeBody]; ok {
		o := *options
		o.ExcludeRequestBody = true
		options = &o
	}

	// Validate request
	requestValidationInput := &openapi3filter.RequestValidationInput{
		Request:    r,
		PathParams: pathParams,
		Route:      route,
		Options:    options,
	}
	if err := openapi3filter.ValidateRequest(r.Context(), requestValidationInput); err != nil {
		var reqErr *openapi3filter.RequestError
		if errors.As(err, &reqErr) {
			return http.StatusBadRequest, err
		}
		var seqErr *openapi3filter.SecurityRequirementsError
		if errors.As(err, &seqErr) {
			return http.StatusUnauthorized, err
		}
		return http.StatusInternalServerError, err
	}
	return http.StatusOK, nil
}

// InvalidAPIEndpointHandler returns ErrInvalidAPIEndpoint, and is currently being used to ensure
// that routes under the pattern it is used with in chi.Router.Mount (i.e. /api/v1) are
// not accessible.
func InvalidAPIEndpointHandler(w http.ResponseWriter, r *http.Request) {
	writeError(w, r, http.StatusInternalServerError, ErrInvalidAPIEndpoint)
}
