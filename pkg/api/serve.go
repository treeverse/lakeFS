package api

//go:generate oapi-codegen -package api -generate "types,client,chi-server,spec" -templates tmpl -o lakefs.gen.go ../../api/swagger.yml

import (
	"net/http"

	chimiddleware "github.com/deepmap/oapi-codegen/pkg/chi-middleware"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/go-chi/chi"
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
)

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
	gatewayDomain string,
) http.Handler {
	logger.Info("initialize OpenAPI server")
	swagger, err := GetSwagger()
	if err != nil {
		panic(err)
	}
	r := chi.NewRouter()
	apiRouter := r.With(
		chimiddleware.OapiRequestValidatorWithOptions(swagger, &chimiddleware.Options{
			Options: openapi3filter.Options{
				AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
			},
		}),
		RequestCounterMiddleware(),
		AuthMiddleware(logger, authService),
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

	uiHandler := NewUIHandler(authService, gatewayDomain)
	r.Mount("/_health", httputil.ServeHealth())
	r.Mount("/metrics", promhttp.Handler())
	r.Mount("/_pprof/", httputil.ServePPROF("/_pprof/"))
	r.Mount("/", uiHandler)
	return r
}
