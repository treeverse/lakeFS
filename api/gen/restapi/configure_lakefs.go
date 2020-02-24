// This file is safe to edit. Once it exists it will not be overwritten

package restapi

import (
	"crypto/tls"
	"net/http"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	"github.com/go-openapi/runtime/middleware"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/branches"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/commits"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/objects"
	"github.com/treeverse/lakefs/api/gen/restapi/operations/repositories"
)

//go:generate swagger generate server --target ../../gen --name Lakefs --spec ../../../swagger.yml --principal models.User --exclude-main

func configureFlags(api *operations.LakefsAPI) {
	// api.CommandLineOptionsGroups = []swag.CommandLineOptionsGroup{ ... }
}

func configureAPI(api *operations.LakefsAPI) http.Handler {
	// configure the api here
	api.ServeError = errors.ServeError

	// Set your custom logger if needed. Default one is log.Printf
	// Expected interface func(string, ...interface{})
	//
	// Example:
	// api.Logger = log.Printf

	api.JSONConsumer = runtime.JSONConsumer()
	api.MultipartformConsumer = runtime.DiscardConsumer

	api.BinProducer = runtime.ByteStreamProducer()
	api.JSONProducer = runtime.JSONProducer()

	// Applies when the Authorization header is set with the Basic scheme
	if api.BasicAuthAuth == nil {
		api.BasicAuthAuth = func(user string, pass string) (*models.User, error) {
			return nil, errors.NotImplemented("basic auth  (basic_auth) has not yet been implemented")
		}
	}

	// Set your custom authorizer if needed. Default one is security.Authorized()
	// Expected interface runtime.Authorizer
	//
	// Example:
	// api.APIAuthorizer = security.Authorized()
	if api.CommitsCommitHandler == nil {
		api.CommitsCommitHandler = commits.CommitHandlerFunc(func(params commits.CommitParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation commits.Commit has not yet been implemented")
		})
	}
	if api.BranchesCreateBranchHandler == nil {
		api.BranchesCreateBranchHandler = branches.CreateBranchHandlerFunc(func(params branches.CreateBranchParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation branches.CreateBranch has not yet been implemented")
		})
	}
	if api.RepositoriesCreateRepositoryHandler == nil {
		api.RepositoriesCreateRepositoryHandler = repositories.CreateRepositoryHandlerFunc(func(params repositories.CreateRepositoryParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation repositories.CreateRepository has not yet been implemented")
		})
	}
	if api.BranchesDeleteBranchHandler == nil {
		api.BranchesDeleteBranchHandler = branches.DeleteBranchHandlerFunc(func(params branches.DeleteBranchParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation branches.DeleteBranch has not yet been implemented")
		})
	}
	if api.ObjectsDeleteObjectHandler == nil {
		api.ObjectsDeleteObjectHandler = objects.DeleteObjectHandlerFunc(func(params objects.DeleteObjectParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation objects.DeleteObject has not yet been implemented")
		})
	}
	if api.RepositoriesDeleteRepositoryHandler == nil {
		api.RepositoriesDeleteRepositoryHandler = repositories.DeleteRepositoryHandlerFunc(func(params repositories.DeleteRepositoryParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation repositories.DeleteRepository has not yet been implemented")
		})
	}
	if api.BranchesDiffBranchHandler == nil {
		api.BranchesDiffBranchHandler = branches.DiffBranchHandlerFunc(func(params branches.DiffBranchParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation branches.DiffBranch has not yet been implemented")
		})
	}
	if api.BranchesDiffBranchesHandler == nil {
		api.BranchesDiffBranchesHandler = branches.DiffBranchesHandlerFunc(func(params branches.DiffBranchesParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation branches.DiffBranches has not yet been implemented")
		})
	}
	if api.BranchesGetBranchHandler == nil {
		api.BranchesGetBranchHandler = branches.GetBranchHandlerFunc(func(params branches.GetBranchParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation branches.GetBranch has not yet been implemented")
		})
	}
	if api.CommitsGetBranchCommitLogHandler == nil {
		api.CommitsGetBranchCommitLogHandler = commits.GetBranchCommitLogHandlerFunc(func(params commits.GetBranchCommitLogParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation commits.GetBranchCommitLog has not yet been implemented")
		})
	}
	if api.CommitsGetCommitHandler == nil {
		api.CommitsGetCommitHandler = commits.GetCommitHandlerFunc(func(params commits.GetCommitParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation commits.GetCommit has not yet been implemented")
		})
	}
	if api.ObjectsGetObjectHandler == nil {
		api.ObjectsGetObjectHandler = objects.GetObjectHandlerFunc(func(params objects.GetObjectParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation objects.GetObject has not yet been implemented")
		})
	}
	if api.RepositoriesGetRepositoryHandler == nil {
		api.RepositoriesGetRepositoryHandler = repositories.GetRepositoryHandlerFunc(func(params repositories.GetRepositoryParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation repositories.GetRepository has not yet been implemented")
		})
	}
	if api.BranchesListBranchesHandler == nil {
		api.BranchesListBranchesHandler = branches.ListBranchesHandlerFunc(func(params branches.ListBranchesParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation branches.ListBranches has not yet been implemented")
		})
	}
	if api.ObjectsListObjectsHandler == nil {
		api.ObjectsListObjectsHandler = objects.ListObjectsHandlerFunc(func(params objects.ListObjectsParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation objects.ListObjects has not yet been implemented")
		})
	}
	if api.RepositoriesListRepositoriesHandler == nil {
		api.RepositoriesListRepositoriesHandler = repositories.ListRepositoriesHandlerFunc(func(params repositories.ListRepositoriesParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation repositories.ListRepositories has not yet been implemented")
		})
	}
	if api.BranchesRevertBranchHandler == nil {
		api.BranchesRevertBranchHandler = branches.RevertBranchHandlerFunc(func(params branches.RevertBranchParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation branches.RevertBranch has not yet been implemented")
		})
	}
	if api.ObjectsStatObjectHandler == nil {
		api.ObjectsStatObjectHandler = objects.StatObjectHandlerFunc(func(params objects.StatObjectParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation objects.StatObject has not yet been implemented")
		})
	}
	if api.ObjectsUploadObjectHandler == nil {
		api.ObjectsUploadObjectHandler = objects.UploadObjectHandlerFunc(func(params objects.UploadObjectParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation objects.UploadObject has not yet been implemented")
		})
	}

	api.PreServerShutdown = func() {}

	api.ServerShutdown = func() {}

	return setupGlobalMiddleware(api.Serve(setupMiddlewares))
}

// The TLS configuration before HTTPS server starts.
func configureTLS(tlsConfig *tls.Config) {
	// Make all necessary changes to the TLS configuration here.
}

// As soon as server is initialized but not run yet, this function will be called.
// If you need to modify a config, store server instance to stop it individually later, this is the place.
// This function can be called multiple times, depending on the number of serving schemes.
// scheme value will be set accordingly: "http", "https" or "unix"
func configureServer(s *http.Server, scheme, addr string) {
}

// The middleware configuration is for the handler executors. These do not apply to the swagger.json document.
// The middleware executes after routing but before authentication, binding and validation
func setupMiddlewares(handler http.Handler) http.Handler {
	return handler
}

// The middleware configuration happens before anything, this middleware also applies to serving the swagger.json document.
// So this is a good place to plug in a panic handling middleware, logging and metrics
func setupGlobalMiddleware(handler http.Handler) http.Handler {
	return handler
}
