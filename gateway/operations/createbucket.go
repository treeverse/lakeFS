package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/index"

	"github.com/gorilla/mux"
)

type CreateBucket struct{}

func (controller *CreateBucket) GetArn() string {
	return "arn:treeverse:repos:::*"
}

func (controller *CreateBucket) GetPermission() permissions.Permission {
	return permissions.ManageRepos
}

func (controller *CreateBucket) Handle(o *AuthenticatedOperation) {
	res := o.ResponseWriter
	repoId := mux.Vars(o.Request)["repo"] // TODO: move this logic elsewhere, a handler shouldn't know about mux
	err := o.Index.CreateRepo(repoId, index.DefaultBranch)
	if err != nil {
		o.Log().WithField("repo", repoId).WithError(err).Error("failed to create repo")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	o.Log().WithField("repo", repoId).Info("repo created successfully")
	res.WriteHeader(http.StatusCreated)
}
