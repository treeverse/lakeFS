package operations

import (
	"errors"
	"net/http"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
	gwerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
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
	if o.HandleUnsupported(w, req, "events") {
		return
	}

	o.Incr("list_repos", o.Principal, "", "")

	parms := req.URL.Query()
	prefix := parms.Get("prefix")

	// Fetch effective policies for filtering
	var opts []catalog.ListRepositoriesOptionsFunc
	policies, _, err := o.Auth.ListEffectivePolicies(req.Context(), o.Principal, &model.PaginationParams{Amount: -1})
	if err != nil && !errors.Is(err, auth.ErrNotImplemented) {
		_ = o.EncodeError(w, req, err, gwerrors.Codes.ToAPIErr(gwerrors.ErrInternalError))
		return
	}
	if err == nil {
		opts = append(opts, catalog.WithListReposPermissionFilter(o.Principal, policies))
	}

	buckets := make([]serde.Bucket, 0)
	var after string
	for {
		// list repositories
		repos, hasMore, err := o.Catalog.ListRepositories(req.Context(), -1, prefix, "", after, opts...)
		if err != nil {
			_ = o.EncodeError(w, req, err, gwerrors.Codes.ToAPIErr(gwerrors.ErrInternalError))
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
