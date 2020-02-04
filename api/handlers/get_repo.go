package handlers

import (
	"net/http"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/go-openapi/runtime/middleware"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/db"
	"golang.org/x/xerrors"
)

func NewGetRepositoryHandler(serverContext ServerContext) operations.GetRepositoryHandler {
	return &getRepoHandler{serverContext}
}

type getRepoHandler struct {
	serverContext ServerContext
}

func (h *getRepoHandler) Handle(params operations.GetRepositoryParams, user *models.User) middleware.Responder {
	err := authorize(h.serverContext, user, permissions.ManageRepos, repoArn("*"))
	if err != nil {
		return operations.NewGetRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
	}

	repo, err := h.serverContext.GetIndex().GetRepo(params.RepositoryID)
	if err != nil && xerrors.Is(err, db.ErrNotFound) {
		return operations.NewGetRepositoryNotFound().
			WithPayload(responseError("repository not found"))
	} else if err != nil {
		return operations.NewGetRepositoryDefault(http.StatusInternalServerError).
			WithPayload(responseError("error fetching repository"))
	}

	return operations.NewGetRepositoryOK().
		WithPayload(&models.Repository{
			BucketName:    repo.GetRepoId(), // TODO: replace with actual bucket name
			CreationDate:  repo.GetCreationDate(),
			DefaultBranch: repo.GetDefaultBranch(),
			ID:            repo.GetRepoId(),
		})
}
