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

func NewDeleteBranchHandler(serverContext ServerContext) operations.DeleteBranchHandler {
	return &deleteBranchHandler{serverContext}
}

type deleteBranchHandler struct {
	serverContext ServerContext
}

func (h *deleteBranchHandler) Handle(params operations.DeleteBranchParams, user *models.User) middleware.Responder {
	err := authorize(h.serverContext, user, permissions.ManageRepos, repoArn(params.RepositoryID))
	if err != nil {
		return operations.NewDeleteBranchUnauthorized().WithPayload(responseErrorFrom(err))
	}

	err = h.serverContext.GetIndex().DeleteBranch(params.RepositoryID, params.BranchID)
	if err != nil && xerrors.Is(err, db.ErrNotFound) {
		return operations.NewDeleteBranchNotFound().
			WithPayload(responseError("branch not found"))
	} else if err != nil {
		return operations.NewDeleteBranchDefault(http.StatusInternalServerError).
			WithPayload(responseError("error fetching branch"))
	}

	return operations.NewDeleteRepositoryNoContent()
}
