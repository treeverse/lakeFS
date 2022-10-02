package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/pkg/permissions"

	"github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
)

type ListBuckets struct{}

func (controller *ListBuckets) RequiredPermissions(_ *http.Request) (permissions.Node, error) {
	return permissions.Node{
		Permission: permissions.Permission{
			Action:   permissions.ListRepositoriesAction,
			Resource: "*",
		},
	}, nil
}

func (controller *ListBuckets) Handle(w http.ResponseWriter, req *http.Request, o *AuthorizedOperation) {
	o.Incr("list_repos", o.Principal, "", "")
	repos, _, err := o.Catalog.ListRepositories(req.Context(), -1, "", "")
	if err != nil {
		_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
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
	o.EncodeResponse(w, req, serde.ListAllMyBucketsResult{
		Buckets: serde.Buckets{Bucket: buckets},
	}, http.StatusOK)
}
