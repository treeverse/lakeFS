package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/gateway/utils"

	"github.com/treeverse/lakefs/permissions"
)

type HeadBucket struct{}

func (controller *HeadBucket) Action(req *http.Request) permissions.Action {
	return permissions.GetRepo(utils.GetRepo(req))
}

func (controller *HeadBucket) Handle(o *RepoOperation) {
	o.ResponseWriter.WriteHeader(http.StatusOK)
}
