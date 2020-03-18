package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/permissions"
)

type HeadBucket struct{}

func (controller *HeadBucket) Action(repoId, refId, path string) permissions.Action {
	return permissions.GetRepo(repoId)
}

func (controller *HeadBucket) Handle(o *RepoOperation) {
	o.ResponseWriter.WriteHeader(http.StatusOK)
}
