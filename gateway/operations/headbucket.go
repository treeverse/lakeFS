package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/permissions"
)

type HeadBucket struct{}

func (controller *HeadBucket) RequiredPermissions(_ *http.Request, repoId string) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.ReadRepositoryAction,
			Resource: permissions.RepoArn(repoId),
		},
	}, nil
}

func (controller *HeadBucket) Handle(o *RepoOperation) {
	o.Incr("get_repo")
	o.ResponseWriter.WriteHeader(http.StatusOK)
}
