package operations

import (
	"net/http"
	"treeverse-lake/gateway/permissions"
)

type HeadBucket struct{}

func (controller *HeadBucket) GetArn() string {
	return "arn:treeverse:repos:::{repo}"
}

func (controller *HeadBucket) GetPermission() string {
	return permissions.PermissionReadRepo
}

func (controller *HeadBucket) Handle(o *RepoOperation) {
	o.ResponseWriter.WriteHeader(http.StatusOK)
}
