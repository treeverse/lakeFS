package handlers

import (
	"net/http"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/go-openapi/runtime/middleware"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
)

func NewListRepositoriesHandler(serverContext ServerContext) operations.ListRepositoriesHandler {
	return &listReposHandler{serverContext}
}

type listReposHandler struct {
	serverContext ServerContext
}

func (h *listReposHandler) Handle(params operations.ListRepositoriesParams, user *models.User) middleware.Responder {
	err := authorize(h.serverContext, user, permissions.ManageRepos, repoArn("*"))
	if err != nil {
		return operations.NewListRepositoriesUnauthorized().WithPayload(responseErrorFrom(err))
	}

	repos, err := h.serverContext.GetIndex().ListRepos()
	if err != nil {
		return operations.NewListRepositoriesDefault(http.StatusInternalServerError).
			WithPayload(responseError("could not list repositories"))
	}

	repoList := make([]*models.Repository, len(repos))
	for i, repo := range repos {
		repoList[i] = &models.Repository{
			BucketName:    repo.RepoId, // TODO: replace this with actual stored field that doesn't exist yet
			CreationDate:  repo.GetCreationDate(),
			DefaultBranch: repo.GetDefaultBranch(),
			ID:            repo.GetRepoId(),
		}
	}
	return operations.NewListRepositoriesOK().WithPayload(repoList)
}
