package operations

import (
	"versio-index/gateway/permissions"
)

type ListObjects struct{}

func (controller *ListObjects) GetArn() string {
	return "arn:versio:repos:::{bucket}"
}

func (controller *ListObjects) GetPermission() string {
	return permissions.PermissionReadRepo
}

func (controller *ListObjects) Handle(o *RepoOperation) {

}
