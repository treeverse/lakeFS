package api

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/loads"
	"github.com/rakyll/statik/fs"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/httputil"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/logging"
	_ "github.com/treeverse/lakefs/statik"
	"github.com/treeverse/lakefs/stats"
)

const (
	RequestIdHeaderName = "X-Request-ID"
	LoggerServiceName   = "rest_api"
)

var (
	ErrAuthenticationFailed = errors.New(http.StatusUnauthorized, "error authenticating request")
)

type Server struct {
	meta index.Index
	//multipartManager index.MultipartManager
	blockStore  block.Adapter
	authService auth.Service
	stats       stats.Collector
	migrator    db.Migrator
	apiServer   *restapi.Server
	handler     *http.ServeMux
	server      *http.Server
}

func NewServer(
	meta index.Index,
	//multipartManager index.MultipartManager,
	blockStore block.Adapter,
	authService auth.Service,
	stats stats.Collector,
	migrator db.Migrator,
) *Server {
	return &Server{
		meta:        meta,
		blockStore:  blockStore,
		authService: authService,
		stats:       stats,
		migrator:    migrator,
	}
}

func (s *Server) DownloadToken() func(string) (*models.User, error) {
	return func(token string) (*models.User, error) {
		return ValidateToken(s.authService, token, time.Now())
	}
}

// BasicAuth returns a function that hooks into Swagger's basic Auth provider
// it uses the Auth.Service provided to ensure credentials are valid
func (s *Server) BasicAuth() func(accessKey, secretKey string) (user *models.User, err error) {
	logger := logging.Default().WithField("Auth", "basic")
	return func(accessKey, secretKey string) (user *models.User, err error) {
		credentials, err := s.authService.GetAPICredentials(accessKey)
		if err != nil {
			logger.WithError(err).WithField("access_key", accessKey).Warn("could not get access key for login")
			return nil, ErrAuthenticationFailed
		}
		if !strings.EqualFold(secretKey, credentials.AccessSecretKey) {
			logger.WithField("access_key", accessKey).Warn("access key secret does not match")
			return nil, ErrAuthenticationFailed
		}
		if credentials.Type != model.CredentialTypeUser {
			return nil, ErrAuthenticationFailed // TODO: support application auth
		}
		userData, err := s.authService.GetUser(*credentials.UserId)
		if err != nil {
			logger.WithField("access_key", accessKey).Warn("could not find user for key pair")
			return nil, ErrAuthenticationFailed
		}
		return &models.User{ID: int64(userData.Id)}, nil
	}
}

func (s *Server) setupHandler(api http.Handler, ui http.Handler, setup http.Handler) {
	mux := http.NewServeMux()
	// api handler
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("User-Agent"), "Mozilla") {
			// this is a browser, pass a response writer that ignores www-authenticate
			w = NonAuthenticatingResponseWriter{w}
		}
		api.ServeHTTP(w, r)
	})
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
	api.DownloadTokenAuth = s.DownloadToken()

	// bind our handlers to the server
	NewHandler(s.meta, s.authService, s.blockStore, s.stats).Configure(api)

	// setup host/port
	s.apiServer = restapi.NewServer(api)
	s.apiServer.ConfigureAPI()

	// serve embedded frontend filesystem
	statikFS, err := fs.NewWithNamespace("webui")
	if err != nil {
		return err
	}

	s.setupHandler(
		// api handler
		httputil.LoggingMiddleWare(
			RequestIdHeaderName,
			logging.Fields{"service_name": LoggerServiceName},
			s.apiServer.GetHandler(),
		),
		// ui handler
		HandlerWithDefault(statikFS, http.FileServer(statikFS), "/"),
		// setup handler
		httputil.LoggingMiddleWare(
			RequestIdHeaderName,
			logging.Fields{"service_name": LoggerServiceName},
			setupLakeFSHandler(s.authService, s.migrator),
		),
	)

	return nil
}

// Serve starts an HTTP server at the given host and port
func (s *Server) Serve(listenAddr string) error {
	handler, err := s.Handler()
	if err != nil {
		return err
	}
	s.server = &http.Server{
		Addr:    listenAddr,
		Handler: handler,
	}
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
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
