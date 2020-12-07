package mvcc

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/catalog"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_CreateRepo(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	type args struct {
		name    string
		storage string
		branch  string
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name:    "basic",
			args:    args{name: "repo1", storage: "s3://bucket1", branch: "master"},
			wantErr: nil,
		},
		{
			name:    "unknown branch",
			args:    args{name: "repo3", storage: "s3://bucket3", branch: ""},
			wantErr: catalog.ErrInvalidValue,
		},
		{
			name:    "missing repo",
			args:    args{name: "", storage: "s3://bucket1", branch: "master"},
			wantErr: catalog.ErrInvalidValue,
		},
		{
			name:    "missing storage",
			args:    args{name: "repo1", storage: "", branch: "master"},
			wantErr: catalog.ErrInvalidValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, err := c.CreateRepository(ctx, tt.args.name, tt.args.storage, tt.args.branch)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("CreateRepository() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if repo != nil {
					t.Fatalf("CreateRepository() repo = %v, expected nil on error", repo)
				}
				return
			}
			if repo.Name != tt.args.name {
				t.Fatalf("CreateRepository() repo name=%s, expected=%s", repo.Name, tt.args.name)
			}
			if repo.DefaultBranch != tt.args.branch {
				t.Fatalf("CreateRepository() repo default branch=%s, expected=%s", repo.DefaultBranch, tt.args.branch)
			}
			if repo.StorageNamespace != tt.args.storage {
				t.Fatalf("CreateRepository() repo storage=%s, expected=%s", repo.StorageNamespace, tt.args.storage)
			}
			// get repository information and verify we got what we created
			rep, err := c.GetRepository(ctx, tt.args.name)
			testutil.MustDo(t, "Get repository for create repository verification", err)
			if rep.Name != tt.args.name {
				t.Errorf("Create repository got name = %s, expected = %s", rep.Name, tt.args.name)
			}
			if rep.DefaultBranch != tt.args.branch {
				t.Errorf("Create repository got branch = %s, expected = %s", rep.DefaultBranch, tt.args.branch)
			}
			if rep.StorageNamespace != tt.args.storage {
				t.Errorf("Create repository got branch = %s, expected = %s", rep.StorageNamespace, tt.args.storage)
			}
			// get initial commit record
			ref, err := c.GetBranchReference(ctx, tt.args.name, tt.args.branch)
			testutil.MustDo(t, "Get branch reference for new repository", err)
			if ref == "" {
				t.Fatal("Create repository should create commit with valid reference")
			}
		})
	}
}
