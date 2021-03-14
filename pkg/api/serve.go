package api

//go:generate oapi-codegen -package api -generate "types,client,chi-server,spec" -o lakefs.gen.go ../../api/swagger.yml

import (
	"net/http"

	chimiddleware "github.com/deepmap/oapi-codegen/pkg/chi-middleware"
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
	//NewCookieAPIHandler(handler))
	//apiRouter := r.With(middleware.RequestID, middleware.Logger, middleware.Recoverer, chimiddleware.OapiRequestValidator(swagger))
	apiRouter := r.With(
		RequestCounterMiddleware(),
		AuthMiddleware(logger, authService),
		chimiddleware.OapiRequestValidator(swagger),
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
	HandlerFromMux(controller, apiRouter)

	uiHandler := NewUIHandler(authService, gatewayDomain)
	r.Mount("/_health", httputil.ServeHealth())
	r.Mount("/metrics", promhttp.Handler())
	r.Mount("/_pprof/", httputil.ServePPROF("/_pprof/"))
	r.Mount("/", uiHandler)

	return r
	/*
		api.Logger = func(msg string, ctx ...interface{}) {
			logging.Default().WithField("logger", "swagger").Debugf(msg, ctx)
		}
		api.BasicAuthAuth = NewBasicAuthHandler(authService)
		api.JwtTokenAuth = NewJwtTokenAuthHandler(authService)

		api.UseSwaggerUI()

		api.ServeError = errors.ServeError

		api.JSONConsumer = runtime.JSONConsumer()
		api.MultipartformConsumer = runtime.DiscardConsumer

		api.BinProducer = runtime.ByteStreamProducer()
		api.JSONProducer = runtime.JSONProducer()
	*/
	// bind our handlers to the server

	/*
		apiHandler := api.Serve(func(handler http.Handler) http.Handler {
			// build handler for our REST API
			return httputil.LoggingMiddleware(
				RequestIDHeaderName,
				logging.Fields{"service_name": LoggerServiceName},
				promhttp.InstrumentHandlerCounter(requestCounter,
					MetricsHandler(api.Context(),
						NewCookieAPIHandler(handler))))
		})
	*/
}

//func authenticateJWTToken(ctx context.Context, logger logging.Logger, authService auth.Service, r *http.Request) *model.User {
//	r.Header.Get("Authentication")
//	claims := &jwt.StandardClaims{}
//	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
//		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
//			return nil, fmt.Errorf("%w: %s", ErrUnexpectedSigningMethod, token.Header["alg"])
//		}
//		return authService.SecretStore().SharedSecret(), nil
//	})
//	if err != nil {
//		return nil, ErrAuthenticationFailed
//	}
//	claims, ok := token.Claims.(*jwt.StandardClaims)
//	if !ok || !token.Valid {
//		return nil, ErrAuthenticationFailed
//	}
//	cred, err := authService.GetCredentials(ctx, claims.Subject)
//	if err != nil {
//		logger.WithField("subject", claims.Subject).Debug("could not find credentials for token")
//		return nil, ErrAuthenticationFailed
//	}
//	userData, err := authService.GetUserByID(ctx, cred.UserID)
//	if err != nil {
//		logger.WithFields(logging.Fields{
//			"user_id": cred.UserID,
//			"subject": claims.Subject,
//		}).Debug("could not find user id by credentials")
//		return nil, ErrAuthenticationFailed
//	}
//	return &models.User{
//		ID: userData.Username,
//	}, nil
//}
