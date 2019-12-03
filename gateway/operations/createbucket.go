package operations

import (
	"net/http"
	"treeverse-lake/gateway/errors"
	"treeverse-lake/gateway/permissions"
	"treeverse-lake/index"
)

type CreateBucket struct{}

func (controller *CreateBucket) GetArn() string {
	return "arn:treeverse:repos:::*"
}

func (controller *CreateBucket) GetPermission() string {
	return permissions.PermissionManageRepos
}

func (controller *CreateBucket) Handle(o *RepoOperation) {
	res := o.ResponseWriter
	err := o.Index.CreateRepo(o.Repo, index.DefaultBranch)
	if err != nil {
		o.Log().WithField("repo", o.Repo).WithError(err).Error("failed to create repo")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	res.WriteHeader(http.StatusCreated)
}
