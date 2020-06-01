package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_CreateBranch(t *testing.T) {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)

	if err := c.CreateRepo(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repo for testing", err)
	}

	type args struct {
		repo         string
		branch       string
		sourceBranch string
	}
	tests := []struct {
		name           string
		args           args
		wantBranchName string
		wantErr        bool
	}{
		{
			name:           "new",
			args:           args{repo: "repo1", branch: "b1", sourceBranch: "master"},
			wantBranchName: "b1",
			wantErr:        false,
		},
		{
			name:           "unknown source",
			args:           args{repo: "repo1", branch: "b2", sourceBranch: "unknown"},
			wantBranchName: "",
			wantErr:        true,
		},
		{
			name:           "unknown repo",
			args:           args{repo: "repo1", branch: "b3", sourceBranch: "unknown"},
			wantBranchName: "",
			wantErr:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.CreateBranch(ctx, tt.args.repo, tt.args.branch, tt.args.sourceBranch)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateBranch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantBranchName != "" && (got == nil || got.Name != tt.wantBranchName) {
				t.Errorf("CreateBranch() got = %+v, want branch name %s", got, tt.wantBranchName)
			}
		})
	}
}

func TestCataloger_CreateBranchOfBranch(t *testing.T) {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)

	if err := c.CreateRepo(ctx, "repo", "bucket", "branch0"); err != nil {
		t.Fatal("create repo for testing", err)
	}
	for i := 1; i < 3; i++ {
		branchName := fmt.Sprintf("branch%d", i)
		sourceBranchName := fmt.Sprintf("branch%d", i-1)
		b, err := c.CreateBranch(ctx, "repo", branchName, sourceBranchName)
		if err != nil {
			t.Fatal("failed to create branch1 based on master", err)
		}
		if b.Name != branchName {
			t.Errorf("CreateBranch name %s, expected %s", b.Name, branchName)
		}
	}
}
