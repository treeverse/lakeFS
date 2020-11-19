package mvcc

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_CreateBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")

	type args struct {
		repository   string
		branch       string
		sourceBranch string
	}
	tests := []struct {
		name              string
		args              args
		wantBranchName    string
		wantCommitMessage string
		wantErr           bool
	}{
		{
			name:              "new",
			args:              args{repository: repo, branch: "b1", sourceBranch: "master"},
			wantCommitMessage: "Branch 'b1' created, source branch 'master'",
			wantBranchName:    "b1",
			wantErr:           false,
		},
		{
			name:           "self",
			args:           args{repository: repo, branch: "master", sourceBranch: "master"},
			wantBranchName: "",
			wantErr:        true,
		},
		{
			name:           "unknown source",
			args:           args{repository: repo, branch: "b2", sourceBranch: "unknown"},
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
			commitLog, err := c.CreateBranch(ctx, tt.args.repository, tt.args.branch, tt.args.sourceBranch)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreateBranch() error = %s, wantErr %t", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if commitLog == nil {
				t.Fatal("CreateBranch() no error, missing commit log")
			} else if tt.wantCommitMessage != commitLog.Message {
				t.Fatalf("CreateBranch() commit log '%s', expected '%s'", commitLog.Message, tt.wantCommitMessage)
			}
		})
	}
}

func TestCataloger_CreateBranch_OfBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "branch0")
	for i := 1; i < 3; i++ {
		branchName := fmt.Sprintf("branch%d", i)
		sourceBranchName := fmt.Sprintf("branch%d", i-1)
		commitLog, err := c.CreateBranch(ctx, repository, branchName, sourceBranchName)
		if err != nil {
			t.Fatalf("failed to create branch '%s' based on '%s': %s", branchName, sourceBranchName, err)
		}
		reference, err := c.GetBranchReference(ctx, repository, branchName)
		if err != nil {
			t.Fatal("Failed to get branch reference after creation:", err)
		}
		if reference == "" {
			t.Errorf("Created branch '%s' should have valid reference to initial commit", branchName)
		}
		if reference != commitLog.Reference {
			t.Errorf("CreateBranch commit reference doesn't match to last branch reference")
		}
	}
}

func TestCataloger_CreateBranch_Existing(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")
	const branchName = "master2"
	_, err := c.CreateBranch(ctx, repo, branchName, "master")
	testutil.MustDo(t, "create test branch", err)

	_, err = c.CreateBranch(ctx, repo, branchName, "master")
	if err == nil {
		t.Fatalf("CreateBranch expected to fail on create branch '%s' already exists", branchName)
	}
}

func TestCataloger_CreateBranch_Parallel(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repo := testCatalogerRepo(t, ctx, c, "repo", "master")

	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			branchName := "branch" + strconv.Itoa(id)
			_, err := c.CreateBranch(ctx, repo, branchName, "master")
			testutil.MustDo(t, "create test branch "+branchName, err)
		}(i)
	}
	wg.Wait()
}
