package api

import (
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/httputil"

	"github.com/go-openapi/loads"
	"github.com/treeverse/lakefs/api/gen/restapi"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/index"
)

const (
	RequestIdHeaderName = "X-Request-ID"
	LoggerServiceName   = "rest_api"
)

type Server struct {
	region           string
	meta             index.Index
	multipartManager index.MultipartManager
	blockStore       block.Adapter
	authService      auth.Service
}

func NewServer(
	region string,
	meta index.Index,
	multipartManager index.MultipartManager,
	blockStore block.Adapter,
	authService auth.Service,
) *Server {
	return &Server{
		region:           region,
		meta:             meta,
		multipartManager: multipartManager,
		blockStore:       blockStore,
		authService:      authService,
	}
}

// BasicAuth returns a function that hooks into Swagger's basic auth provider
// it uses the auth.Service provided to ensure credentials are valid
func (s *Server) BasicAuth() func(accessKey, secretKey string) (user *models.User, err error) {
	return func(accessKey, secretKey string) (user *models.User, err error) {
		credentials, err := s.authService.GetAPICredentials(accessKey)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(secretKey, credentials.GetAccessSecretKey()) {
			return nil, fmt.Errorf("authentication error")
		}
		userData, err := s.authService.GetUser(credentials.GetEntityId())
		if err != nil {
			return nil, err
		}
		return &models.User{ID: userData.GetId()}, nil
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

	api.BasicAuthAuth = s.BasicAuth()

	// bind our handlers to the server
	NewHandler(s.meta, s.authService, s.blockStore).Configure(api)

	// setup host/port
	srv := restapi.NewServer(api)
	srv.ConfigureAPI()

	return srv, nil
}

// Serve starts an HTTP server at the given host and port
func (s *Server) Serve(host string, port int) error {
	srv, err := s.SetupServer()
	if err != nil {
		return err
	}

	// add logging to every request
	next := srv.GetHandler()
	srv.SetHandler(httputil.LoggingMiddleWare(RequestIdHeaderName, LoggerServiceName, next))

	// setup listen address
	srv.Host = host
	srv.Port = port

	if err := srv.Serve(); err != nil {
		return err
	}
	return nil
}
