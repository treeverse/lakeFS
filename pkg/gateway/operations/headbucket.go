package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/pkg/permissions"
)

type HeadBucket struct{}

func (controller *HeadBucket) RequiredPermissions(_ *http.Request, repoID string) (permissions.Node, error) {
	return permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ReadRepositoryAction,
			Resource: permissions.RepoArn(repoID),
		},
	}, nil
}

func (controller *HeadBucket) Handle(w http.ResponseWriter, _ *http.Request, o *RepoOperation) {
	o.Incr("get_repo", o.Principal, o.Repository.Name, "")
	w.WriteHeader(http.StatusOK)
}
