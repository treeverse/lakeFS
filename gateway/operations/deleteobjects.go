package operations

import (
	"versio-index/gateway/permissions"
)

type DeleteObjects struct{}

func (controller *DeleteObjects) GetArn() string {
	return "arn:versio:repos:::{bucket}"
}

func (controller *DeleteObjects) GetPermission() string {
	return permissions.PermissionWriteRepo
}

func (controller *DeleteObjects) Handle(o *RepoOperation) {

}
