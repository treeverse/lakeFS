package handlers

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/permissions"
	"golang.org/x/xerrors"
)

func NewDeleteRepositoryHandler(serverContext ServerContext) operations.DeleteRepositoryHandler {
	return &deleteRepoHandler{serverContext}
}

type deleteRepoHandler struct {
	serverContext ServerContext
}

func (h *deleteRepoHandler) Handle(params operations.DeleteRepositoryParams, user *models.User) middleware.Responder {
	err := authorize(h.serverContext, user, permissions.ManageRepos, repoArn("*"))
	if err != nil {
		return operations.NewDeleteRepositoryUnauthorized().WithPayload(responseErrorFrom(err))
	}

	err = h.serverContext.GetIndex().DeleteRepo(params.RepositoryID)
	if err != nil && xerrors.Is(err, db.ErrNotFound) {
		return operations.NewDeleteRepositoryNotFound().
			WithPayload(responseError("repository not found"))
	} else if err != nil {
		return operations.NewDeleteRepositoryDefault(http.StatusInternalServerError).
			WithPayload(responseError("error deleting repository"))
	}

	return operations.NewDeleteRepositoryNoContent()
}
