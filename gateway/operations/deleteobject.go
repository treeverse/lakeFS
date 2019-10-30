package operations

import (
	"versio-index/gateway/permissions"
)

type DeleteObject struct{}

func (controller *DeleteObject) GetArn() string {
	return "arn:versio:repos:::{bucket}"
}

func (controller *DeleteObject) GetPermission() string {
	return permissions.PermissionManageRepos
}

func (controller *DeleteObject) Handle(o *PathOperation) {

}
