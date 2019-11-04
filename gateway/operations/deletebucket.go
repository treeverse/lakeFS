package operations

import (
	"versio-index/gateway/errors"
	"versio-index/gateway/permissions"
)

type DeleteBucket struct{}

func (controller *DeleteBucket) GetArn() string {
	return "arn:versio:repos:::*"
}

func (controller *DeleteBucket) GetPermission() string {
	return permissions.PermissionManageRepos
}

func (controller *DeleteBucket) Handle(o *RepoOperation) {
	err := o.Index.DeleteRepo(o.ClientId, o.Repo)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
}
