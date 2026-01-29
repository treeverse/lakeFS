package operations

import (
	"context"
	"errors"
	"net/http"

	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/catalog"
	gwerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/gateway/serde"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// EffectivePoliciesLister is an interface for services that can list effective policies.
// This is used for RBAC-based filtering in ListBuckets.
type EffectivePoliciesLister interface {
	ListEffectivePolicies(ctx context.Context, username string, params *model.PaginationParams) ([]*model.Policy, *model.Paginator, error)
}

type ListBuckets struct{}

func (controller *ListBuckets) RequiredPermissions(_ *http.Request) (permissions.Node, error) {
	// Return empty permissions here because the standard authorization model requires
	// a specific resource (e.g., "*"), which would fail for users with pattern-based policies.
	// Instead, we verify the user has ListRepositories permission on at least one resource
	// in the Handle method, then apply RBAC-based filtering to return only accessible repos.
	return permissions.Node{}, nil
}

// Handle - list buckets (repositories)
func (controller *ListBuckets) Handle(w http.ResponseWriter, req *http.Request, o *AuthorizedOperation) {
	if o.HandleUnsupported(w, req, "events") {
		return
	}

	o.Incr("list_repos", o.Principal, "", "")

	ctx := req.Context()
	parms := req.URL.Query()
	prefix := parms.Get("prefix")

	// Get user for policy-based filtering
	user, err := auth.GetUser(ctx)
	if err != nil {
		_ = o.EncodeError(w, req, err, gwerrors.Codes.ToAPIErr(gwerrors.ErrAccessDenied))
		return
	}

	// Try to get effective policies for filtering.
	// If the auth service supports it, apply RBAC-based filtering.
	// Otherwise, fall back to returning all repositories (backward compatible).
	var opts []catalog.ListRepositoriesOptionsFunc
	if policyLister, ok := o.Auth.(EffectivePoliciesLister); ok {
		policies, _, err := policyLister.ListEffectivePolicies(ctx, user.Username, &model.PaginationParams{Amount: -1})
		if err != nil && !errors.Is(err, auth.ErrNotImplemented) {
			o.Log(req).WithError(err).Error("failed to list effective policies")
			_ = o.EncodeError(w, req, err, gwerrors.Codes.ToAPIErr(gwerrors.ErrInternalError))
			return
		}
		if err == nil {
			// Verify the user has at least one ListRepositories permission.
			// This ensures users without any repository access get AccessDenied
			// rather than an empty list.
			if !auth.HasActionOnAnyResource(policies, permissions.ListRepositoriesAction) {
				_ = o.EncodeError(w, req, nil, gwerrors.Codes.ToAPIErr(gwerrors.ErrAccessDenied))
				return
			}
			opts = append(opts, catalog.WithListReposPermissionFilter(user.Username, policies))
		}
	}

	buckets := make([]serde.Bucket, 0)
	var after string
	for {
		// list repositories with filtering
		repos, hasMore, err := o.Catalog.ListRepositories(ctx, -1, prefix, "", after, opts...)
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
