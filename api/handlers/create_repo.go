package handlers

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/permissions"
)

func NewCreateRepositoryHandler(serverContext ServerContext) operations.CreateRepositoryHandler {
	return &createRepoHandler{serverContext}
}

type createRepoHandler struct {
	serverContext ServerContext
}

func (h *createRepoHandler) Handle(params operations.CreateRepositoryParams, user *models.User) middleware.Responder {
	err := authorize(h.serverContext, user, permissions.ManageRepos, repoArn("*"))
	if err != nil {
		return operations.NewCreateRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
	}

	err = h.serverContext.GetIndex().CreateRepo(params.RepositoryID, params.Repository.DefaultBranch)
	if err != nil {
		return operations.NewGetRepositoryDefault(http.StatusInternalServerError).
			WithPayload(responseError("error creating repository"))
	}

	repo, err := h.serverContext.GetIndex().GetRepo(params.RepositoryID)
	if err != nil {
		return operations.NewGetRepositoryDefault(http.StatusInternalServerError).
			WithPayload(responseError("error creating repository"))
	}

	return operations.NewCreateRepositoryCreated().WithPayload(&models.Repository{
		BucketName:    repo.GetRepoId(),
		CreationDate:  repo.GetCreationDate(),
		DefaultBranch: repo.GetDefaultBranch(),
		ID:            repo.GetRepoId(),
	})
}
