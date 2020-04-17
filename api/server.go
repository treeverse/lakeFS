package api

import (
	"net/http"
	"strings"
	"time"

	"github.com/treeverse/lakefs/auth/model"

	"github.com/treeverse/lakefs/logging"

	"github.com/go-openapi/errors"

	"github.com/rakyll/statik/fs"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/httputil"

	"github.com/go-openapi/loads"
	"github.com/treeverse/lakefs/api/gen/restapi"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/index"

	_ "github.com/treeverse/lakefs/statik"
)

const (
	RequestIdHeaderName = "X-Request-ID"
	LoggerServiceName   = "rest_api"
)

var (
	ErrAuthenticationFailed = errors.New(http.StatusUnauthorized, "error authenticating request")
)

type Server struct {
	meta             index.Index
	multipartManager index.MultipartManager
	blockStore       block.Adapter
	authService      auth.Service
}

func NewServer(
	meta index.Index,
	multipartManager index.MultipartManager,
	blockStore block.Adapter,
	authService auth.Service,
) *Server {
	return &Server{
		meta:             meta,
		multipartManager: multipartManager,
		blockStore:       blockStore,
		authService:      authService,
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

// SetupServer returns a Server that has been configured with basic authenticator and is registered
// to all relevant API handlers
func (s *Server) SetupServer() (*restapi.Server, error) {
	swaggerSpec, err := loads.Analyzed(restapi.SwaggerJSON, "")
	if err != nil {
		return nil, err
	}

	api := operations.NewLakefsAPI(swaggerSpec)
	api.Logger = func(msg string, ctx ...interface{}) {
		logging.Default().WithField("logger", "swagger").Debugf(msg, ctx)
	}

	api.BasicAuthAuth = s.BasicAuth()
	api.DownloadTokenAuth = s.DownloadToken()

	// bind our handlers to the server
	NewHandler(s.meta, s.authService, s.blockStore).Configure(api)

	// setup host/port
	srv := restapi.NewServer(api)
	srv.ConfigureAPI()

	return srv, nil
}

// Serve starts an HTTP server at the given host and port
func (s *Server) Serve(listenAddr string) error {
	srv, err := s.SetupServer()
	if err != nil {
		return err
	}

	// serve embedded frontend filesystem
	statikFS, err := fs.NewWithNamespace("webui")
	if err != nil {
		return err
	}

	httpServer := http.Server{
		Addr: listenAddr,
		Handler: HandlerWithUI(
			httputil.LoggingMiddleWare(RequestIdHeaderName, logging.Fields{"service_name": LoggerServiceName},
				srv.GetHandler(), // api
			),
			HandlerWithDefault(statikFS, http.FileServer(statikFS), "/"), // ui
		),
	}

	return httpServer.ListenAndServe()
}
