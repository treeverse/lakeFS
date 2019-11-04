package operations

import (
	"versio-index/gateway/errors"
	"versio-index/gateway/permissions"
)

type DeleteObject struct{}

func (controller *DeleteObject) GetArn() string {
	return "arn:treeverse:repos:::{bucket}"
}

func (controller *DeleteObject) GetPermission() string {
	return permissions.PermissionManageRepos
}

func (controller *DeleteObject) Handle(o *PathOperation) {
	err := o.Index.DeleteObject(o.ClientId, o.Repo, o.Branch, o.Path)
	if err != nil {
		o.Log().WithError(err).Error("could not delete key")
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}
}
