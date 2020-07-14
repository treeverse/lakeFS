package api

import (
	"context"
	"fmt"
	"net/http"

	"gopkg.in/dgrijalva/jwt-go.v3"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/loads"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/logging"
	_ "github.com/treeverse/lakefs/statik"
	"github.com/treeverse/lakefs/stats"
)

const (
	RequestIdHeaderName        = "X-Request-ID"
	LoggerServiceName          = "rest_api"
	JWTAuthorizationHeaderName = "X-JWT-Authorization"
)

var (
	ErrAuthenticationFailed = errors.New(http.StatusUnauthorized, "error authenticating request")
)

type Server struct {
	meta        auth.MetadataManager
	index       index.Index
	blockStore  block.Adapter
	authService auth.Service
	stats       stats.Collector
	migrator    db.Migrator
	apiServer   *restapi.Server
	handler     *http.ServeMux
	server      *http.Server
	logger      logging.Logger
}

func NewServer(
	index index.Index,
	blockStore block.Adapter,
	authService auth.Service,
	meta auth.MetadataManager,
	stats stats.Collector,
	migrator db.Migrator,
	logger logging.Logger,
) *Server {
	logger.Info("initialized OpenAPI server")
	return &Server{
		index:       index,
		blockStore:  blockStore,
		authService: authService,
		meta:        meta,
		stats:       stats,
		migrator:    migrator,
		logger:      logger,
	}
}

// JwtTokenAuth decodes, validates and authenticates a user that exists
// in the X-JWT-Authorization header.
// This header either exists natively, or is set using a token
func (s *Server) JwtTokenAuth() func(string) (*models.User, error) {
	logger := logging.Default().WithField("auth", "jwt")
	return func(tokenString string) (*models.User, error) {
		claims := &jwt.StandardClaims{}
		token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
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
func (s *Server) BasicAuth() func(accessKey, secretKey string) (user *models.User, err error) {
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
		userData, err := s.authService.GetUserById(credentials.UserId)
		if err != nil {
			logger.WithField("access_key", accessKey).Warn("could not find user for key pair")
			return nil, ErrAuthenticationFailed
		}
		return &models.User{
			ID: userData.DisplayName,
		}, nil
	}
}

func (s *Server) setupHandler(api http.Handler, ui http.Handler, setup http.Handler) {
	mux := http.NewServeMux()
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

// setupServer returns a Server that has been configured with basic authenticator and is registered
// to all relevant API handlers
func (s *Server) setupServer() error {
	swaggerSpec, err := loads.Analyzed(restapi.SwaggerJSON, "")
	if err != nil {
		return err
	}

	api := operations.NewLakefsAPI(swaggerSpec)
	api.Logger = func(msg string, ctx ...interface{}) {
		logging.Default().WithField("logger", "swagger").Debugf(msg, ctx)
	}
	api.BasicAuthAuth = s.BasicAuth()
	api.JwtTokenAuth = s.JwtTokenAuth()

	// bind our handlers to the server
	NewHandler(s.index, s.authService, s.blockStore, s.stats, s.logger).Configure(api)

	// setup host/port
	s.apiServer = restapi.NewServer(api)
	s.apiServer.ConfigureAPI()

	s.setupHandler(
		// api handler
		httputil.LoggingMiddleware(
			RequestIdHeaderName,
			logging.Fields{"service_name": LoggerServiceName},
			cookieToAPIHeader(s.apiServer.GetHandler()),
		),

		// ui handler
		UIHandler(s.authService),

		// setup handler
		httputil.LoggingMiddleware(
			RequestIdHeaderName,
			logging.Fields{"service_name": LoggerServiceName},
			setupLakeFSHandler(s.authService, s.meta, s.migrator, s.stats),
		),
	)

	return nil
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

// Listen starts an HTTP server at the given host and port
func (s *Server) Listen(listenAddr string) error {
	handler, err := s.Handler()
	if err != nil {
		return err
	}
	s.server = &http.Server{
		Addr:    listenAddr,
		Handler: handler,
	}
	logging.Default().
		WithField("listen_address", listenAddr).
		Info("started OpenAPI server")
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.server.SetKeepAlivesEnabled(false)
	return s.server.Shutdown(ctx)
}

func (s *Server) Handler() (http.Handler, error) {
	if s.handler != nil {
		return s.handler, nil
	}
	err := s.setupServer()
	if err != nil {
		return nil, err
	}
	return s.handler, nil
}
