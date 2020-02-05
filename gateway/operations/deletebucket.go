package operations

import (
	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/permissions"
)

type DeleteBucket struct{}

func (controller *DeleteBucket) GetArn() string {
	return "arn:treeverse:repos:::*"
}

func (controller *DeleteBucket) GetPermission() permissions.Permission {
	return permissions.ManageRepos
}

func (controller *DeleteBucket) Handle(o *RepoOperation) {
	err := o.Index.DeleteRepo(o.Repo.GetRepoId())
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
}
