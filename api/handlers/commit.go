package handlers

import (
	"net/http"

	"github.com/treeverse/lakefs/ident"

	"github.com/go-openapi/runtime/middleware"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/permissions"
)

func NewCommitHandler(serverContext ServerContext) operations.CommitHandler {
	return &commitHandler{serverContext}
}

type commitHandler struct {
	serverContext ServerContext
}

func (h *commitHandler) Handle(params operations.CommitParams, user *models.User) middleware.Responder {
	err := authorize(h.serverContext, user, permissions.ManageRepos, repoArn(params.RepositoryID))
	if err != nil {
		return operations.NewCommitUnauthorized().WithPayload(responseErrorFrom(err))
	}
	commit, err := h.serverContext.GetIndex().Commit(
		params.RepositoryID,
		params.BranchID,
		*params.Commit.Message,
		user.ID,
		params.Commit.Metadata,
	)

	if err != nil {
		return operations.NewCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
	}
	return operations.NewCommitCreated().WithPayload(&models.Commit{
		Committer:    commit.GetCommitter(),
		CreationDate: commit.GetTimestamp(),
		ID:           ident.Hash(commit),
		Message:      commit.GetMessage(),
		Metadata:     commit.GetMetadata(),
		Parents:      commit.GetParents(),
	})
}
