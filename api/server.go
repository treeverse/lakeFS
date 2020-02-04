package api

import (
	"fmt"
	"strings"

	"github.com/go-openapi/loads"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/api/handlers"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/index"
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

func (s *Server) GetRegion() string {
	return s.region
}

func (s *Server) GetIndex() index.Index {
	return s.meta
}

func (s *Server) GetMultipartManager() index.MultipartManager {
	return s.multipartManager
}

func (s *Server) GetBlockStore() block.Adapter {
	return s.blockStore
}

func (s *Server) GetAuthService() auth.Service {
	return s.authService
}

func (s *Server) bind(api *operations.LakefsAPI) {
	// Register operations here
	api.ListRepositoriesHandler = handlers.NewListRepositoriesHandler(s)
	api.GetRepositoryHandler = handlers.NewGetRepositoryHandler(s)
	api.CreateRepositoryHandler = handlers.NewCreateRepositoryHandler(s)
	api.DeleteRepositoryHandler = handlers.NewDeleteRepositoryHandler(s)

	api.ListBranchesHandler = handlers.NewListBranchesHandler(s)
	api.GetBranchHandler = handlers.NewGetBranchHandler(s)
	api.CreateBranchHandler = handlers.NewCreateBranchHandler(s)
	api.DeleteBranchHandler = handlers.NewDeleteBranchHandler(s)

}

func (s *Server) Serve(host string, port int) error {
	swaggerSpec, err := loads.Analyzed(restapi.SwaggerJSON, "")
	if err != nil {
		return err
	}

	api := operations.NewLakefsAPI(swaggerSpec)

	api.BasicAuthAuth = func(accessKey, secretKey string) (user *models.User, err error) {
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

	// bind our handlers to the server
	s.bind(api)

	// attach authentication

	srv := restapi.NewServer(api)
	srv.Host = host
	srv.Port = port

	srv.ConfigureAPI()

	if err := srv.Serve(); err != nil {
		return err
	}
	return nil
}
