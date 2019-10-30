package operations

import (
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

}
