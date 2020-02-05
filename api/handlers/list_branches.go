package handlers

import (
	"net/http"

	"github.com/go-openapi/runtime/middleware"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/permissions"
)

func NewListBranchesHandler(serverContext ServerContext) operations.ListBranchesHandler {
	return &listBranchesHandler{serverContext}
}

type listBranchesHandler struct {
	serverContext ServerContext
}

func (h *listBranchesHandler) Handle(params operations.ListBranchesParams, user *models.User) middleware.Responder {
	err := authorize(h.serverContext, user, permissions.ManageRepos, repoArn(params.RepositoryID))
	if err != nil {
		return operations.NewListRepositoriesUnauthorized().WithPayload(responseErrorFrom(err))
	}

	branches, err := h.serverContext.GetIndex().ListBranches(params.RepositoryID, -1)
	if err != nil {
		return operations.NewListRepositoriesDefault(http.StatusInternalServerError).
			WithPayload(responseError("could not list branches"))
	}

	branchList := make([]*models.Refspec, len(branches))
	for i, branch := range branches {
		branchList[i] = &models.Refspec{
			CommitID: &branch.Commit,
			ID:       &branch.Name,
		}
	}
	return operations.NewListBranchesOK().WithPayload(branchList)
}
