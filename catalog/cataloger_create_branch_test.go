package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_CreateBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	testutil.MustDo(t, "test repository for create branch",
		c.CreateRepository(ctx, "repo1", "bucket1", "master"))
	_, err := c.CreateBranch(ctx, "repo1", "master2", "master")
	testutil.MustDo(t, "create test branch for create branch test", err)

	type args struct {
		repository   string
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
			args:           args{repository: "repo1", branch: "b1", sourceBranch: "master"},
			wantBranchName: "b1",
			wantErr:        false,
		},
		{
			name:           "self",
			args:           args{repository: "repo1", branch: "master", sourceBranch: "master"},
			wantBranchName: "",
			wantErr:        true,
		},
		{
			name:           "existing",
			args:           args{repository: "repo1", branch: "master2", sourceBranch: "master"},
			wantBranchName: "",
			wantErr:        true,
		},
		{
			name:           "unknown source",
			args:           args{repository: "repo1", branch: "b2", sourceBranch: "unknown"},
			wantBranchName: "",
			wantErr:        true,
		},
		{
			name:           "unknown repository",
			args:           args{repository: "repo1", branch: "b3", sourceBranch: "unknown"},
			wantBranchName: "",
			wantErr:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.CreateBranch(ctx, tt.args.repository, tt.args.branch, tt.args.sourceBranch)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateBranch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil && got <= 0 {
				t.Errorf("CreateBranch() branch ID = %s, wanted >0", err)
			}
		})
	}
}

func TestCataloger_CreateBranchOfBranch(t *testing.T) {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)
	repository := testCatalogerRepo(t, ctx, c, "repository", "branch0")
	for i := 1; i < 3; i++ {
		branchName := fmt.Sprintf("branch%d", i)
		sourceBranchName := fmt.Sprintf("branch%d", i-1)
		id, err := c.CreateBranch(ctx, repository, branchName, sourceBranchName)
		if err != nil {
			t.Fatalf("failed to create branch '%s' based on '%s': %s", branchName, sourceBranchName, err)
		}
		if id <= 0 {
			t.Errorf("CreateBranch ID %d, expected >0 for branch: %s", id, branchName)
		}
		b, err := c.GetBranch(ctx, repository, branchName)
		if err != nil {
			t.Error("Branch not found after create:", err)
		}
		if b.Name != branchName {
			t.Errorf("Created branch name doesn't match %s: expected %s", b.Name, branchName)
		}
	}
}
