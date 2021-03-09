package api

//go:generate swagger generate client -q -A lakefs -f ../../api/swagger.yml -P models.User -t gen
//go:generate swagger generate server -q -A lakefs -f ../../api/swagger.yml -P models.User -t gen --exclude-main

import (
	"net/http"

	"github.com/go-openapi/loads"
	"github.com/go-swagger/go-swagger/examples/oauth2/restapi"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/treeverse/lakefs/pkg/api/gen/restapi/operations"
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
	swaggerSpec, _ := loads.Analyzed(restapi.SwaggerJSON, "")

	api := operations.NewLakefsAPI(swaggerSpec)
	api.Logger = func(msg string, ctx ...interface{}) {
		logging.Default().WithField("logger", "swagger").Debugf(msg, ctx)
	}
	api.BasicAuthAuth = NewBasicAuthHandler(authService)
	api.JwtTokenAuth = NewJwtTokenAuthHandler(authService)

	// bind our handlers to the server
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
	controller.Configure(api)

	api.UseSwaggerUI()

	apiHandler := api.Serve(func(handler http.Handler) http.Handler {
		// build handler for our REST API
		return httputil.LoggingMiddleware(
			RequestIDHeaderName,
			logging.Fields{"service_name": LoggerServiceName},
			promhttp.InstrumentHandlerCounter(requestCounter,
				MetricsHandler(api.Context(),
					NewCookieAPIHandler(handler))))
	})
	uiHandler := NewUIHandler(authService, gatewayDomain)

	mux := http.NewServeMux()
	mux.Handle("/_health", httputil.ServeHealth())
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/_pprof/", httputil.ServePPROF("/_pprof/"))
	mux.Handle("/api/", apiHandler)
	mux.Handle("/swagger.json", apiHandler)
	mux.Handle("/", uiHandler)
	return mux
}
