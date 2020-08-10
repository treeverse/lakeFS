//go:generate swagger generate client -q -A lakefs -f ../swagger.yml -P models.User -t gen
//go:generate swagger generate server -q -A lakefs -f ../swagger.yml -P models.User -t gen --exclude-main
package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	openapierr "github.com/go-openapi/errors"
	"github.com/go-openapi/loads"
	"github.com/go-openapi/runtime/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/dedup"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/retention"
	_ "github.com/treeverse/lakefs/statik"
	"github.com/treeverse/lakefs/stats"
	"gopkg.in/dgrijalva/jwt-go.v3"
)

const (
	RequestIDHeaderName        = "X-Request-ID"
	LoggerServiceName          = "rest_api"
	JWTAuthorizationHeaderName = "X-JWT-Authorization"
)

var (
	ErrAuthenticationFailed    = openapierr.New(http.StatusUnauthorized, "error authenticating request")
	ErrUnexpectedSigningMethod = errors.New("unexpected signing method")
)

type Handler struct {
	meta         auth.MetadataManager
	cataloger    catalog.Cataloger
	blockStore   block.Adapter
	authService  auth.Service
	stats        stats.Collector
	retention    retention.Service
	migrator     db.Migrator
	apiServer    *restapi.Server
	handler      *http.ServeMux
	dedupCleaner *dedup.Cleaner
	logger       logging.Logger
}

func NewHandler(
	cataloger catalog.Cataloger,
	blockStore block.Adapter,
	authService auth.Service,
	meta auth.MetadataManager,
	stats stats.Collector,
	retention retention.Service,
	migrator db.Migrator,
	dedupCleaner *dedup.Cleaner,
	logger logging.Logger,
) http.Handler {
	logger.Info("initialized OpenAPI server")
	s := &Handler{
		cataloger:    cataloger,
		blockStore:   blockStore,
		authService:  authService,
		meta:         meta,
		stats:        stats,
		retention:    retention,
		migrator:     migrator,
		dedupCleaner: dedupCleaner,
		logger:       logger,
	}
	s.buildAPI()
	return s.handler
}

// JwtTokenAuth decodes, validates and authenticates a user that exists
// in the X-JWT-Authorization header.
// This header either exists natively, or is set using a token
func (s *Handler) JwtTokenAuth() func(string) (*models.User, error) {
	logger := logging.Default().WithField("auth", "jwt")
	return func(tokenString string) (*models.User, error) {
		claims := &jwt.StandardClaims{}
		token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("%w: %s", ErrUnexpectedSigningMethod, token.Header["alg"])
			}

			return s.authService.SecretStore().SharedSecret(), nil
		})
		if err != nil {
			return nil, ErrAuthenticationFailed
		}
		claims, ok := token.Claims.(*jwt.StandardClaims)
		if !ok || !token.Valid {
			return nil, ErrAuthenticationFailed
		}
		userData, err := s.authService.GetUser(claims.Subject)
		if err != nil {
			logger.WithField("subject", claims.Subject).Warn("could not find user for token")
			return nil, ErrAuthenticationFailed
		}
		return &models.User{
			ID: userData.DisplayName,
		}, nil
	}
}

// BasicAuth returns a function that hooks into Swagger's basic Auth provider
// it uses the Auth.Service provided to ensure credentials are valid
func (s *Handler) BasicAuth() func(accessKey, secretKey string) (user *models.User, err error) {
	logger := logging.Default().WithField("auth", "basic")
	return func(accessKey, secretKey string) (user *models.User, err error) {
		credentials, err := s.authService.GetCredentials(accessKey)
		if err != nil {
			logger.WithError(err).WithField("access_key", accessKey).Warn("could not get access key for login")
			return nil, ErrAuthenticationFailed
		}
		if secretKey != credentials.AccessSecretKey {
			logger.WithField("access_key", accessKey).Warn("access key secret does not match")
			return nil, ErrAuthenticationFailed
		}
		userData, err := s.authService.GetUserByID(credentials.UserID)
		if err != nil {
			logger.WithField("access_key", accessKey).Warn("could not find user for key pair")
			return nil, ErrAuthenticationFailed
		}
		return &models.User{
			ID: userData.DisplayName,
		}, nil
	}
}

func (s *Handler) setupHandler(api http.Handler, ui http.Handler, setup http.Handler) {
	mux := http.NewServeMux()
	// health check
	mux.Handle("/_health", httputil.ServeHealth())
	// metrics
	mux.Handle("/metrics", promhttp.Handler())
	// pprof endpoint
	mux.Handle("/_pprof/", httputil.ServePPROF("/_pprof/"))
	// api handler
	mux.Handle("/api/", api)
	// swagger
	mux.Handle("/swagger.json", api)
	// setup system
	mux.Handle(SetupLakeFSRoute, setup)
	// otherwise, serve  UI
	mux.Handle("/", ui)

	s.handler = mux
}

// buildAPI wires together the JWT and basic authenticator and registers all relevant API handlers
func (s *Handler) buildAPI() {
	swaggerSpec, _ := loads.Analyzed(restapi.SwaggerJSON, "")

	api := operations.NewLakefsAPI(swaggerSpec)
	api.Logger = func(msg string, ctx ...interface{}) {
		logging.Default().WithField("logger", "swagger").Debugf(msg, ctx)
	}
	api.BasicAuthAuth = s.BasicAuth()
	api.JwtTokenAuth = s.JwtTokenAuth()
	// bind our handlers to the server
	NewController(s.cataloger, s.authService, s.blockStore, s.stats, s.retention, s.dedupCleaner, s.logger).Configure(api)

	// setup host/port
	s.apiServer = restapi.NewServer(api)
	s.apiServer.ConfigureAPI()
	s.setupHandler(
		// api handler
		httputil.LoggingMiddleware(
			RequestIDHeaderName,
			logging.Fields{"service_name": LoggerServiceName},
			promhttp.InstrumentHandlerCounter(requestCounter,
				metricsMiddleware(api.Context(),
					cookieToAPIHeader(
						s.apiServer.GetHandler(),
					)),
			),
		),

		// ui handler
		UIHandler(s.authService),

		// setup handler
		httputil.LoggingMiddleware(
			RequestIDHeaderName,
			logging.Fields{"service_name": LoggerServiceName},
			setupLakeFSHandler(s.authService, s.meta, s.migrator, s.stats),
		),
	)
}

func cookieToAPIHeader(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// read cookie (no need to validate, this will be done in the API
		cookie, err := r.Cookie(JWTCookieName)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		// header found
		r.Header.Set(JWTAuthorizationHeaderName, cookie.Value)
		next.ServeHTTP(w, r)
	})
}

func metricsMiddleware(ctx *middleware.Context, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route, _, ok := ctx.RouteInfo(r)
		start := time.Now()
		mrw := httputil.NewMetricResponseWriter(w)
		next.ServeHTTP(mrw, r)
		if ok {
			requestHistograms.
				WithLabelValues(route.Operation.ID, strconv.Itoa(mrw.StatusCode)).
				Observe(time.Since(start).Seconds())
		}
	})
}
