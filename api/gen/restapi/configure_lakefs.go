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
	if api.CreateBranchHandler == nil {
		api.CreateBranchHandler = operations.CreateBranchHandlerFunc(func(params operations.CreateBranchParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation operations.CreateBranch has not yet been implemented")
		})
	}
	if api.CreateRepositoryHandler == nil {
		api.CreateRepositoryHandler = operations.CreateRepositoryHandlerFunc(func(params operations.CreateRepositoryParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation operations.CreateRepository has not yet been implemented")
		})
	}
	if api.DeleteBranchHandler == nil {
		api.DeleteBranchHandler = operations.DeleteBranchHandlerFunc(func(params operations.DeleteBranchParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation operations.DeleteBranch has not yet been implemented")
		})
	}
	if api.DeleteRepositoryHandler == nil {
		api.DeleteRepositoryHandler = operations.DeleteRepositoryHandlerFunc(func(params operations.DeleteRepositoryParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation operations.DeleteRepository has not yet been implemented")
		})
	}
	if api.GetBranchHandler == nil {
		api.GetBranchHandler = operations.GetBranchHandlerFunc(func(params operations.GetBranchParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation operations.GetBranch has not yet been implemented")
		})
	}
	if api.GetRepositoryHandler == nil {
		api.GetRepositoryHandler = operations.GetRepositoryHandlerFunc(func(params operations.GetRepositoryParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation operations.GetRepository has not yet been implemented")
		})
	}
	if api.ListBranchesHandler == nil {
		api.ListBranchesHandler = operations.ListBranchesHandlerFunc(func(params operations.ListBranchesParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation operations.ListBranches has not yet been implemented")
		})
	}
	if api.ListRepositoriesHandler == nil {
		api.ListRepositoriesHandler = operations.ListRepositoriesHandlerFunc(func(params operations.ListRepositoriesParams, principal *models.User) middleware.Responder {
			return middleware.NotImplemented("operation operations.ListRepositories has not yet been implemented")
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
