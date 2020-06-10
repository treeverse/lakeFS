package catalog

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_DeleteBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	if err := c.CreateRepository(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repository for testing", err)
	}
	for i := 0; i < 3; i++ {
		branchName := fmt.Sprintf("branch%d", i)
		var sourceBranch string
		if i == 0 {
			sourceBranch = "master"
		} else {
			sourceBranch = fmt.Sprintf("branch%d", i-1)
		}
		_ = testCatalogerBranch(t, ctx, c, "repo1", branchName, sourceBranch)
	}
	_ = testCatalogerBranch(t, ctx, c, "repo1", "b1", "master")

	if err := c.CreateEntry(ctx, "repo1", "b1", "/file1", "7c9d66ac57c9fa91bb375256fe1541e33f9548904c3f41fcd1e1208f2f3559f1", "/file1abc", 42, nil); err != nil {
		t.Fatal("create entry for testing", err)
	}

	type args struct {
		repository string
		branch     string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "delete default branch",
			args:    args{repository: "repo1", branch: "master"},
			wantErr: true,
		},
		{
			name:    "delete branch",
			args:    args{repository: "repo1", branch: "b1"},
			wantErr: false,
		},
		{
			name:    "delete unknown branch",
			args:    args{repository: "repo1", branch: "nob"},
			wantErr: true,
		},
		{
			name:    "delete without repository",
			args:    args{repository: "repoX", branch: "nob"},
			wantErr: true,
		},
		{
			name:    "delete branch used by branch",
			args:    args{repository: "repo1", branch: "branch1"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.DeleteBranch(ctx, tt.args.repository, tt.args.branch)
			if (err != nil) != tt.wantErr {
				t.Fatalf("DeleteBranch() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			_, err = c.GetBranch(ctx, tt.args.repository, tt.args.branch)
			if !errors.As(err, &db.ErrNotFound) {
				t.Errorf("Branch should not be found after delete, got err=%s", err)
				return
			}
		})
	}
}

func TestCataloger_DeleteBranchTwice(t *testing.T) {
	ctx := context.Background()
	cdb, _ := testutil.GetDB(t, databaseURI, "lakefs_catalog")
	c := NewCataloger(cdb)

	if err := c.CreateRepository(ctx, "repo1", "bucket1", "branch0"); err != nil {
		t.Fatal("create repository for testing", err)
	}

	const numBranches = 3
	// create branches
	for i := 0; i < numBranches; i++ {
		sourceBranchName := fmt.Sprintf("branch%d", i)
		branchName := fmt.Sprintf("branch%d", i+1)
		_ = testCatalogerBranch(t, ctx, c, "repo1", branchName, sourceBranchName)
	}
	// delete twice (checking double delete) in reverse order
	for i := numBranches; i > 0; i-- {
		branchName := fmt.Sprintf("branch%d", i)
		err := c.DeleteBranch(ctx, "repo1", branchName)
		if err != nil {
			t.Fatal("Expected delete to succeed on", branchName, err)
		}
		err = c.DeleteBranch(ctx, "repo1", branchName)
		if err == nil {
			t.Fatal("Expected delete to fail on", branchName, err)
		}
	}
}
