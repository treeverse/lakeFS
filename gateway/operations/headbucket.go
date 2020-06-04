package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/permissions"
)

type HeadBucket struct{}

func (controller *HeadBucket) RequiredPermission(repoId, refId, path string) permissions.Permission {
	return permissions.GetRepo(repoId)
}

func (controller *HeadBucket) Handle(o *RepoOperation) {
	o.Incr("get_repo")
	o.ResponseWriter.WriteHeader(http.StatusOK)
}
