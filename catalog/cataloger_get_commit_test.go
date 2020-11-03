package catalog

import (
	"context"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_GetCommit(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	defer func() { _ = c.Close() }()

	// test data
	const testBranch = "master"
	repository := testCatalogerRepo(t, ctx, c, "repo", testBranch)
	const testCommitsLen = 2
	refs := make([]string, testCommitsLen)
	for i := 0; i < testCommitsLen; i++ {
		n := strconv.Itoa(i)
		testPath := "/file" + n
		meta := Metadata{"k" + n: "v" + n}
		msg := "Commit" + n
		committer := "tester" + n
		testCatalogerCreateEntry(t, ctx, c, repository, testBranch, testPath, meta, "")
		commitLog, err := c.Commit(ctx, repository, testBranch, msg, committer, meta)
		testutil.MustDo(t, "commit "+msg, err)
		refs[i] = commitLog.Reference
	}

	tests := []struct {
		name      string
		reference string
		want      *CommitLog
		wantErr   bool
	}{
		{
			name:      "first",
			reference: "~KJ8Wd1Rs96a",
			want: &CommitLog{
				Reference:    "~KJ8Wd1Rs96a",
				Committer:    "tester0",
				Message:      "Commit0",
				CreationDate: time.Now(),
				Metadata:     Metadata{"k0": "v0"},
				Parents:      []string{"~KJ8Wd1Rs96Z"},
			},
			wantErr: false,
		},
		{
			name:      "second",
			reference: "~KJ8Wd1Rs96b",
			want: &CommitLog{
				Reference:    "~KJ8Wd1Rs96b",
				Committer:    "tester1",
				Message:      "Commit1",
				CreationDate: time.Now(),
				Metadata:     Metadata{"k1": "v1"},
				Parents:      []string{"~KJ8Wd1Rs96a"},
			},
			wantErr: false,
		},
		{
			name:      "unknown",
			reference: "~unknown",
			want:      nil,
			wantErr:   true,
		},
		{
			name:      "empty",
			reference: "",
			want:      nil,
			wantErr:   true,
		},
		{
			name:      "branch uncommitted",
			reference: "master",
			want: &CommitLog{
				Reference:    "~KJ8Wd1Rs96b",
				Committer:    "tester1",
				Message:      "Commit1",
				CreationDate: time.Now(),
				Metadata:     Metadata{"k1": "v1"},
				Parents:      []string{"~KJ8Wd1Rs96a"},
			},
			wantErr: false,
		},
		{
			name:      "branch committed",
			reference: "master:HEAD",
			want: &CommitLog{
				Reference:    "~KJ8Wd1Rs96b",
				Committer:    "tester1",
				Message:      "Commit1",
				CreationDate: time.Now(),
				Metadata:     Metadata{"k1": "v1"},
				Parents:      []string{"~KJ8Wd1Rs96a"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.Now()
			got, err := c.GetCommit(ctx, repository, tt.reference)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCommit() error = %s, wantErr %t", err, tt.wantErr)
				return
			}
			if got != nil {
				if timeDifference(got.CreationDate, now) > 10*time.Second {
					t.Errorf("expected creation time %s, got very different %s", got.CreationDate, now)
				}
				if tt.want != nil {
					got.CreationDate = tt.want.CreationDate
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetCommit() got = %s, want %s", spew.Sdump(got), spew.Sdump(tt.want))
			}
		})
	}
}

func TestCataloger_GetMergeCommit(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	defer func() { _ = c.Close() }()

	repo := testCatalogerRepo(t, ctx, c, "repo", "master")

	// prepare data on master
	for i := 0; i < 3; i++ {
		testCatalogerCreateEntry(t, ctx, c, repo, "master", "/file"+strconv.Itoa(i), nil, "master")
	}
	_, err := c.Commit(ctx, repo, "master", "commit to master", "tester", nil)
	testutil.MustDo(t, "commit to master", err)

	// prepare data on b1
	_, err = c.CreateBranch(ctx, repo, "b1", "master")
	testutil.MustDo(t, "create b1 branch", err)
	for i := 2; i < 6; i++ {
		testCatalogerCreateEntry(t, ctx, c, repo, "b1", "/file"+strconv.Itoa(i), nil, "b1")
	}
	_, err = c.Commit(ctx, repo, "b1", "commit to branch", "tester", nil)
	testutil.MustDo(t, "commit to b1", err)

	// merge b1 to master
	res, err := c.Merge(ctx, repo, "b1", "master", "tester", "merge b1 to master", nil)
	testutil.MustDo(t, "merge b1 to master", err)

	// test commit on master got two parents
	commitLog, err := c.GetCommit(ctx, repo, res.Reference)
	testutil.MustDo(t, "get commit of merge reference", err)

	if len(commitLog.Parents) != 2 {
		t.Fatalf("Expected commit log to include two parents, got %d", len(commitLog.Parents))
	}
	expectedReferences := []string{
		"~3WaKeL",      // branch b1, commit 5
		"~KJ8Wd1Rs96a", // branch master, commit 3
	}
	sort.Strings(expectedReferences)
	sort.Strings(commitLog.Parents)
	if !reflect.DeepEqual(expectedReferences, commitLog.Parents) {
		t.Fatalf("Merged commit log parents %s, expected %s", spew.Sdump(commitLog.Parents), spew.Sdump(expectedReferences))
	}

}
