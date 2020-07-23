package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/gateway/errors"
	"github.com/treeverse/lakefs/gateway/serde"
)

type ListBuckets struct{}

func (controller *ListBuckets) RequiredPermissions(_ *http.Request) ([]permissions.Permission, error) {
	return []permissions.Permission{
		{
			Action:   permissions.ListRepositoriesAction,
			Resource: "*",
		},
	}, nil
}

func (controller *ListBuckets) Handle(o *AuthenticatedOperation) {
	o.Incr("list_repos")
	repos, _, err := o.Cataloger.ListRepositories(o.Context(), -1, "")
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// assemble response
	buckets := make([]serde.Bucket, len(repos))
	for i, repo := range repos {
		buckets[i] = serde.Bucket{
			CreationDate: serde.Timestamp(repo.CreationDate),
			Name:         repo.Name,
		}
	}
	// write response
	o.EncodeResponse(serde.ListAllMyBucketsResult{
		Buckets: serde.Buckets{Bucket: buckets},
	}, http.StatusOK)
}
