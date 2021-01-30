package api

//go:generate swagger generate client -q -A lakefs -f ../swagger.yml -P models.User -t gen
//go:generate swagger generate server -q -A lakefs -f ../swagger.yml -P models.User -t gen --exclude-main

import (
	"net/http"

	"github.com/go-openapi/loads"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/treeverse/lakefs/api/gen/restapi"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/httputil"

	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/parade"
	_ "github.com/treeverse/lakefs/statik"
	"github.com/treeverse/lakefs/stats"
)

const (
	RequestIDHeaderName = "X-Request-ID"
	LoggerServiceName   = "rest_api"
)

func Serve(
	cataloger catalog.Cataloger,
	blockStore block.Adapter,
	authService auth.Service,
	metadataManager auth.MetadataManager,
	stats stats.Collector,
	migrator db.Migrator,
	parade parade.Parade,
	logger logging.Logger,
) http.Handler {
	logger.Info("initialized OpenAPI server")
	swaggerSpec, _ := loads.Analyzed(restapi.SwaggerJSON, "")

	api := operations.NewLakefsAPI(swaggerSpec)
	api.Logger = func(msg string, ctx ...interface{}) {
		logging.Default().WithField("logger", "swagger").Debugf(msg, ctx)
	}
	api.BasicAuthAuth = BasicAuthHandler(authService)
	api.JwtTokenAuth = JwtTokenAuthHandler(authService)

	// bind our handlers to the server
	controller := NewController(cataloger, authService, blockStore, stats, parade, metadataManager, migrator, stats, logger)
	controller.Configure(api)

	api.UseSwaggerUI()
	apiHandler := api.Serve(func(handler http.Handler) http.Handler {
		// build handler for our REST API
		return httputil.LoggingMiddleware(
			RequestIDHeaderName,
			logging.Fields{"service_name": LoggerServiceName},
			promhttp.InstrumentHandlerCounter(requestCounter,
				MetricsHandler(api.Context(),
					CookieAPIHandler(handler))))
	})
	uiHandler := UIHandler(authService)

	mux := http.NewServeMux()
	mux.Handle("/_health", httputil.ServeHealth())
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/_pprof/", httputil.ServePPROF("/_pprof/"))
	mux.Handle("/api/", apiHandler)
	mux.Handle("/swagger.json", apiHandler)
	mux.Handle("/", uiHandler)
	return mux
}
