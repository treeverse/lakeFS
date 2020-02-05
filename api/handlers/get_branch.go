package handlers

import (
	"net/http"

	"github.com/go-openapi/swag"

	"github.com/go-openapi/runtime/middleware"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/permissions"
	"golang.org/x/xerrors"
)

func NewGetBranchHandler(serverContext ServerContext) operations.GetBranchHandler {
	return &getBranchHandler{serverContext}
}

type getBranchHandler struct {
	serverContext ServerContext
}

func (h *getBranchHandler) Handle(params operations.GetBranchParams, user *models.User) middleware.Responder {
	err := authorize(h.serverContext, user, permissions.ManageRepos, repoArn(params.RepositoryID))
	if err != nil {
		return operations.NewGetBranchUnauthorized().WithPayload(responseErrorFrom(err))
	}

	branch, err := h.serverContext.GetIndex().GetBranch(params.RepositoryID, params.BranchID)
	if err != nil && xerrors.Is(err, db.ErrNotFound) {
		return operations.NewGetBranchNotFound().
			WithPayload(responseError("branch not found"))
	} else if err != nil {
		return operations.NewGetBranchDefault(http.StatusInternalServerError).
			WithPayload(responseError("error fetching repository"))
	}

	return operations.NewGetBranchOK().
		WithPayload(&models.Refspec{
			CommitID: swag.String(branch.GetCommit()),
			ID:       swag.String(branch.GetName()),
		})
}
