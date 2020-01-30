package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/permissions"
)

type HeadBucket struct{}

func (controller *HeadBucket) GetArn() string {
	return "arn:treeverse:repos:::{repo}"
}

func (controller *HeadBucket) GetPermission() permissions.Permission {
	return permissions.ReadRepo
}

func (controller *HeadBucket) Handle(o *RepoOperation) {
	o.ResponseWriter.WriteHeader(http.StatusOK)
}
