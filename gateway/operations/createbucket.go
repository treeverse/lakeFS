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
	err := o.Index.CreateRepo(o.ClientId, o.Repo, index.DefaultBranch)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
	res.WriteHeader(http.StatusCreated)
}
