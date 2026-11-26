package operations

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/gateway/path"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// TestListObjectsRequiredPermissions asserts that the permission demanded by RequiredPermissions
// matches the operation the handler actually performs. ListV1 and ListV2 both decide between listing
// branches and listing objects with `!prefix.WithPath`, and neither consults the delimiter, so the
// required permission must be derived from that same predicate. A request that lists branches while
// requiring only fs:ListObjects is an authorization skew: it lets a caller holding fs:ListObjects
// enumerate branch names, which requires fs:ListBranches.
func TestListObjectsRequiredPermissions(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		delimiter string
		want      string
	}{
		// Branch listings. The handler resolves each of these to a bare ref (or no ref at all), so
		// each requires fs:ListBranches regardless of the delimiter.
		{name: "bucket root with delimiter", prefix: "", delimiter: "/", want: permissions.ListBranchesAction},
		{name: "bucket root without delimiter", prefix: "", delimiter: "", want: permissions.ListBranchesAction},
		{name: "bucket root with non slash delimiter", prefix: "", delimiter: "|", want: permissions.ListBranchesAction},
		{name: "partial branch name without delimiter", prefix: "ma", delimiter: "", want: permissions.ListBranchesAction},
		{name: "bare ref with non slash delimiter", prefix: "main", delimiter: "|", want: permissions.ListBranchesAction},
		// ResolvePath strips one optional leading slash (`^/?` in EncodedPathRe), so these are bare
		// refs even though strings.Contains(prefix, "/") is true.
		{name: "bare ref with leading slash", prefix: "/main", delimiter: "/", want: permissions.ListBranchesAction},
		{name: "bare ref with leading slash and no delimiter", prefix: "/main", delimiter: "", want: permissions.ListBranchesAction},

		// Object listings. The prefix resolves to <ref>/<path>, so these require fs:ListObjects.
		{name: "object listing at branch root", prefix: "main/", delimiter: "/", want: permissions.ListObjectsAction},
		{name: "object listing nested", prefix: "main/a/b", delimiter: "/", want: permissions.ListObjectsAction},
		{name: "object listing without delimiter", prefix: "main/a/b", delimiter: "", want: permissions.ListObjectsAction},
		{name: "object listing with leading slash", prefix: "/main/a", delimiter: "/", want: permissions.ListObjectsAction},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/repo", nil)
			q := req.URL.Query()
			q.Set("prefix", tt.prefix)
			if tt.delimiter != "" {
				q.Set("delimiter", tt.delimiter)
			}
			req.URL.RawQuery = q.Encode()

			controller := &ListObjects{}
			node, err := controller.RequiredPermissions(req, "repo")
			if err != nil {
				t.Fatalf("RequiredPermissions: %s", err)
			}
			if got := node.Permission.Action; got != tt.want {
				t.Errorf("prefix=%q delimiter=%q: required permission is %s, want %s",
					tt.prefix, tt.delimiter, got, tt.want)
			}

			// Cross-check the expectation against the predicate ListV1/ListV2 branch on, so a change
			// to either side that makes them disagree fails here rather than silently skewing.
			resolved, err := path.ResolvePath(tt.prefix)
			if err != nil {
				t.Fatalf("ResolvePath(%q): %s", tt.prefix, err)
			}
			wantByHandler := permissions.ListObjectsAction
			if !resolved.WithPath {
				wantByHandler = permissions.ListBranchesAction
			}
			if wantByHandler != tt.want {
				t.Errorf("prefix=%q: handler lists %s, so the hardcoded want %s is stale",
					tt.prefix,
					map[bool]string{true: "branches", false: "objects"}[!resolved.WithPath],
					tt.want)
			}
			if got, want := node.Permission.Resource, permissions.RepoArn("repo"); got != want {
				t.Errorf("prefix=%q delimiter=%q: resource = %s, want %s", tt.prefix, tt.delimiter, got, want)
			}
		})
	}
}

// TestListObjectsRequiredPermissionsSubResources asserts that the bucket-level sub-resources served by
// ListObjects keep requiring fs:ListObjects. Every bucket-level GET is routed to ListObjects and
// RequiredPermissions runs before Handle demuxes them, so deriving the permission from the prefix
// alone would make GetBucketLocation, ListMultipartUploads and GetBucketVersioning require
// fs:ListBranches, and would reject a prefix they ignore. None of them return branch names, so
// leaving them on fs:ListObjects does not reopen the branch-enumeration gap.
func TestListObjectsRequiredPermissionsSubResources(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "location", query: "location"},
		{name: "uploads", query: "uploads"},
		{name: "versioning", query: "versioning"},
		// A prefix is a real ListMultipartUploads parameter, and the others ignore it. In every case
		// the sub-resource decides the permission, not the prefix.
		{name: "location with malformed prefix", query: "location&prefix=%2F"},
		{name: "uploads with malformed prefix", query: "uploads&prefix=%2F"},
		{name: "versioning with malformed prefix", query: "versioning&prefix=%2F"},
		{name: "uploads with bare ref prefix", query: "uploads&prefix=main"},
		{name: "uploads with object prefix", query: "uploads&prefix=main%2Fa"},
		// With delimiter=/ these required fs:ListBranches before this change, because the old
		// heuristic ran for sub-resources too. They now require fs:ListObjects like every other
		// invocation of the same three operations. Nothing new is reachable: the region and the
		// static versioning document already answered to fs:ListObjects without a delimiter, and
		// ListMultipartUploads rejects a delimiter as not implemented.
		{name: "location with delimiter", query: "location&delimiter=%2F"},
		{name: "versioning with delimiter", query: "versioning&delimiter=%2F"},
		{name: "uploads with delimiter", query: "uploads&delimiter=%2F"},
		{name: "versioning with delimiter and prefix", query: "versioning&delimiter=%2F&prefix=main"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/repo?"+tt.query, nil)

			controller := &ListObjects{}
			node, err := controller.RequiredPermissions(req, "repo")
			if err != nil {
				t.Fatalf("query=%q: unexpected error: %s", tt.query, err)
			}
			if got, want := node.Permission.Action, permissions.ListObjectsAction; got != want {
				t.Errorf("query=%q: action = %s, want %s", tt.query, got, want)
			}
		})
	}
}

// TestListObjectsRequiredPermissionsUnsupportedSubResources documents a deliberate behavior change.
// The sub-resources HandleUnsupported rejects are NOT classified by bucketGetKindOf, so they follow
// the listing rule and a bucket-level request for one now requires fs:ListBranches.
//
// This is intentional and must not be "fixed" to fs:ListObjects. gateways.s3.verify_unsupported
// defaults to true, in which case HandleUnsupported answers these with 405 before any listing
// happens, and the only effect is that an fs:ListObjects-only principal sees 403 instead of 405. But
// when it is disabled these fall through to ListV1 and genuinely do list branches, so fs:ListBranches
// is the only requirement that is correct in both configurations. Classifying them as fs:ListObjects
// would reopen this advisory whenever the flag is off.
func TestListObjectsRequiredPermissionsUnsupportedSubResources(t *testing.T) {
	for _, query := range []string{"acl", "tagging", "versions", "policy", "lifecycle", "cors"} {
		t.Run(query, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/repo?"+query, nil)

			controller := &ListObjects{}
			node, err := controller.RequiredPermissions(req, "repo")
			if err != nil {
				t.Fatalf("query=%q: unexpected error: %s", query, err)
			}
			if got, want := node.Permission.Action, permissions.ListBranchesAction; got != want {
				t.Errorf("query=%q: action = %s, want %s", query, got, want)
			}
		})
	}
}

// TestListObjectsRequiredPermissionsMalformedPrefix asserts that a prefix resolving to neither a bare
// ref nor <ref>/<path> fails closed. The operation being requested cannot be determined, so neither
// can the permission it requires: RequiredPermissions must surface the error rather than assume an
// object listing, and the middleware then rejects the request before the handler runs. Returning a
// permission here would make authorization depend on the handler continuing to reject malformed
// prefixes, which is exactly the coupling this advisory removes.
func TestListObjectsRequiredPermissionsMalformedPrefix(t *testing.T) {
	for _, prefix := range []string{"/", "//foo"} {
		t.Run(prefix, func(t *testing.T) {
			if _, err := path.ResolvePath(prefix); err == nil {
				t.Fatalf("ResolvePath(%q) resolved; expected it to be malformed", prefix)
			}

			req := httptest.NewRequest(http.MethodGet, "/repo", nil)
			q := req.URL.Query()
			q.Set("prefix", prefix)
			q.Set("delimiter", "/")
			req.URL.RawQuery = q.Encode()

			controller := &ListObjects{}
			node, err := controller.RequiredPermissions(req, "repo")
			if err == nil {
				t.Fatalf("prefix=%q: expected an error, got required permission %q",
					prefix, node.Permission.Action)
			}
			if !errors.Is(err, path.ErrPathMalformed) {
				t.Errorf("prefix=%q: err = %v, want %v", prefix, err, path.ErrPathMalformed)
			}
			// An empty node means "no permission required" to the middleware (see authorize in
			// pkg/gateway/handler.go), so it must never be returned alongside a nil error.
			if got := node.Permission.Action; got != "" {
				t.Errorf("prefix=%q: action = %q, want empty", prefix, got)
			}
		})
	}
}

type refCatalogStub struct {
	branches, tags    []string
	branchErr, tagErr error
}

func refPage(names []string, prefix string, limit int, after string) ([]string, bool) {
	page := []string{}
	for _, name := range names {
		if name > after && strings.HasPrefix(name, prefix) {
			page = append(page, name)
		}
	}
	return page[:min(len(page), limit)], len(page) > limit
}

func (c *refCatalogStub) ListBranches(_ context.Context, _, prefix string, limit int, after string, _ ...graveler.ListOptionsFunc) ([]*catalog.Branch, bool, error) {
	page, more := refPage(c.branches, prefix, limit, after)
	result := []*catalog.Branch{}
	for _, name := range page {
		result = append(result, &catalog.Branch{Name: name})
	}
	return result, more, c.branchErr
}

func (c *refCatalogStub) ListTags(_ context.Context, _, prefix string, limit int, after string) ([]*catalog.Tag, bool, error) {
	page, more := refPage(c.tags, prefix, limit, after)
	result := []*catalog.Tag{}
	for _, name := range page {
		result = append(result, &catalog.Tag{ID: name})
	}
	return result, more, c.tagErr
}

func TestListReferencesPagination(t *testing.T) {
	for _, limit := range []int{1, 2, 3, 1000} {
		for _, prefix := range []string{"", "v", "missing"} {
			c := &refCatalogStub{branches: []string{"a", "dev", "main", "same"}, tags: []string{"a-1", "same", "v1", "v2"}}
			want := []string{}
			for _, name := range []string{"a", "a-1", "dev", "main", "same", "v1", "v2"} {
				if strings.HasPrefix(name, prefix) {
					want = append(want, name+"/")
				}
			}
			got, after := []string{}, ""
			for page := 0; ; page++ {
				require.Less(t, page, 10, "pagination did not terminate")
				dirs, next, more, err := (&ListObjects{}).listReferences(t.Context(), c, "repo", prefix, limit, after)
				require.NoError(t, err)
				require.LessOrEqual(t, len(dirs), limit)
				for _, dir := range dirs {
					got = append(got, dir.Prefix)
				}
				if !more {
					break
				}
				require.Greater(t, next, after, "cursor must advance")
				after = next
			}
			require.Equal(t, want, got, "limit=%d prefix=%q", limit, prefix)
		}
	}
}

func TestListReferencesErrors(t *testing.T) {
	failure := errors.New("catalog unavailable")
	for _, c := range []*refCatalogStub{{branchErr: failure}, {tagErr: failure}} {
		_, _, _, err := (&ListObjects{}).listReferences(t.Context(), c, "repo", "", 2, "")
		require.ErrorIs(t, err, failure)
	}
}
