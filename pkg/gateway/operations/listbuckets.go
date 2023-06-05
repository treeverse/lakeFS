package operations

import (
	"net/http"

	"github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
	"github.com/treeverse/lakefs/pkg/permissions"
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

// Handle - list buckets (repositories)
func (controller *ListBuckets) Handle(w http.ResponseWriter, req *http.Request, o *AuthorizedOperation) {
	o.Incr("list_repos", o.Principal, "", "")

	buckets := make([]serde.Bucket, 0, 100)
	var after string
	for {
		// list repositories
		repos, hasMore, err := o.Catalog.ListRepositories(req.Context(), -1, "", after)
		if err != nil {
			_ = o.EncodeError(w, req, errors.Codes.ToAPIErr(errors.ErrInternalError))
			return
		}

		// collect repositories
		for _, repo := range repos {
			buckets = append(buckets, serde.Bucket{
				CreationDate: serde.Timestamp(repo.CreationDate),
				Name:         repo.Name,
			})
		}

		if !hasMore || len(repos) == 0 {
			break
		}
		after = repos[len(repos)-1].Name
	}
	// write response
	o.EncodeResponse(w, req, serde.ListAllMyBucketsResult{
		Buckets: serde.Buckets{Bucket: buckets},
	}, http.StatusOK)
}
