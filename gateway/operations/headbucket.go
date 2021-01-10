package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/permissions"
)

type HeadBucket struct{}

func (controller *HeadBucket) RequiredPermissions(_ *http.Request, repoID string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.ReadRepositoryAction,
			Resource: permissions.RepoArn(repoID),
		},
	}, nil
}

func (controller *HeadBucket) Handle(w http.ResponseWriter, _ *http.Request, o *RepoOperation) {
	o.Incr("get_repo")
	w.WriteHeader(http.StatusOK)
}
