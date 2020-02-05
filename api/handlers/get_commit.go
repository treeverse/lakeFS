package handlers

import (
	"net/http"

	"github.com/treeverse/lakefs/db"
	"golang.org/x/xerrors"

	"github.com/go-openapi/runtime/middleware"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/api/gen/restapi/operations"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/permissions"
)

func NewGetCommitHandler(serverContext ServerContext) operations.GetCommitHandler {
	return &getCommitHandler{serverContext}
}

type getCommitHandler struct {
	serverContext ServerContext
}

func (h *getCommitHandler) Handle(params operations.GetCommitParams, user *models.User) middleware.Responder {
	err := authorize(h.serverContext, user, permissions.ManageRepos, repoArn(params.RepositoryID))
	if err != nil {
		return operations.NewGetCommitUnauthorized().WithPayload(responseErrorFrom(err))
	}
	commit, err := h.serverContext.GetIndex().GetCommit(params.RepositoryID, params.CommitID)

	if xerrors.Is(err, db.ErrNotFound) {
		return operations.NewGetCommitNotFound().WithPayload(responseError("commit not found"))
	}
	if err != nil {
		return operations.NewGetCommitDefault(http.StatusInternalServerError).WithPayload(responseErrorFrom(err))
	}
	return operations.NewGetCommitOK().WithPayload(&models.Commit{
		Committer:    commit.GetCommitter(),
		CreationDate: commit.GetTimestamp(),
		ID:           ident.Hash(commit),
		Message:      commit.GetMessage(),
		Metadata:     commit.GetMetadata(),
		Parents:      commit.GetParents(),
	})
}
