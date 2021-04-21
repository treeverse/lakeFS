package api

//go:generate oapi-codegen -package api -generate "types,client,chi-server,spec" -templates tmpl -o lakefs.gen.go ../../api/swagger.yml

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/cloud"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/httputil"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
)

const (
	RequestIDHeaderName = "X-Request-ID"
	LoggerServiceName   = "rest_api"
	BaseURL             = "/api/v1"

	extensionValidationExcludeBody = "x-validation-exclude-body"
)

type responseError struct {
	Message string `json:"message"`
}

func Serve(
	catalog catalog.Interface,
	authService auth.Service,
	blockAdapter block.Adapter,
	metadataManager auth.MetadataManager,
	migrator db.Migrator,
	collector stats.Collector,
	cloudMetadataProvider cloud.MetadataProvider,
	actions actionsHandler,
	logger logging.Logger,
	gatewayDomains []string,
) http.Handler {
	logger.Info("initialize OpenAPI server")
	swagger, err := GetSwagger()
	if err != nil {
		panic(err)
	}
	r := chi.NewRouter()
	apiRouter := r.With(
		OapiRequestValidatorWithOptions(swagger, &openapi3filter.Options{
			AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
		}),
		AuthMiddleware(logger, swagger, authService),
		httputil.LoggingMiddleware(RequestIDHeaderName, logging.Fields{"service_name": LoggerServiceName}),
		MetricsMiddleware(swagger),
	)

	controller := NewController(
		catalog,
		authService,
		blockAdapter,
		metadataManager,
		migrator,
		collector,
		cloudMetadataProvider,
		actions,
		logger,
	)
	HandlerFromMuxWithBaseURL(controller, apiRouter, BaseURL)

	uiHandler := NewUIHandler(authService, gatewayDomains)
	r.Mount("/_health", httputil.ServeHealth())
	r.Mount("/metrics", promhttp.Handler())
	r.Mount("/_pprof/", httputil.ServePPROF("/_pprof/"))
	r.Mount("/swagger.json", http.HandlerFunc(swaggerSpecHandler))
	r.Mount("/", uiHandler)
	return r
}

func swaggerSpecHandler(w http.ResponseWriter, _ *http.Request) {
	reader, err := GetSwaggerSpecReader()
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
// Used our own implementation in order to:
// 1. Use the latest version kin-openapi (can switch back when oapi-codegen will be updated)
// 2. For file upload wanted to skip body validation for two reasons:
//    a. didn't find a way for the validator to accept any file content type
//    b. didn't want the validator to read the complete request body for the specific request
func OapiRequestValidatorWithOptions(swagger *openapi3.Swagger, options *openapi3filter.Options) func(http.Handler) http.Handler {
	router, err := legacy.NewRouter(swagger)
	if err != nil {
		panic(err)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// validate request
			statusCode, err := validateRequest(r, router, options)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Content-Type-Options", "nosniff")
				w.WriteHeader(statusCode)
				_ = json.NewEncoder(w).Encode(responseError{Message: err.Error()})
				return
			}
			// serve
			next.ServeHTTP(w, r)
		})
	}
}

func validateRequest(r *http.Request, router routers.Router, options *openapi3filter.Options) (int, error) {
	// Find route
	route, pathParams, err := router.FindRoute(r)
	if err != nil {
		return http.StatusBadRequest, err // We failed to find a matching route for the request.
	}

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
