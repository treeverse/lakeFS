package mvcc

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_RollbackCommit(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	files := []string{"file1", "file2", "file3"}
	var refs []string
	// commit 3 files
	for _, filename := range files {
		testCatalogerCreateEntry(t, ctx, c, repository, "master", filename, nil, "")

		commitLog, err := c.Commit(ctx, repository, "master", "first", "tester", nil)
		testutil.MustDo(t, "first commit", err)

		refs = append(refs, commitLog.Reference)
	}
	if refs == nil || len(refs) != 3 {
		t.Fatalf("expected 3 references for 3 commits, got %d", len(refs))
	}

	// rollback to each reference and check all files are there
	for i := 0; i < len(refs); i++ {
		filesCount := len(refs) - i
		ref := refs[filesCount-1]
		err := c.RollbackCommit(ctx, repository, ref)
		testutil.MustDo(t, "rollback", err)

		entries, _, err := c.ListEntries(ctx, repository, "master", "", "", "", -1)
		testutil.MustDo(t, "list entries", err)
		if len(entries) != filesCount {
			t.Fatalf("List entries length after revert %d, expected %d", len(entries), filesCount)
		}
		for i := 0; i < filesCount; i++ {
			if entries[i].Path != files[i] {
				t.Fatalf("List entries after revert, file at index %d: %s, expected %s", i, entries[i].Path, files[i])
			}
		}
	}

	// check there are no commits
	commits, _, err := c.ListCommits(ctx, repository, "master", "", -1)
	testutil.MustDo(t, "list commits", err)
	const expectedCommitsLen = 3 // branch + repo + first commit
	if len(commits) != expectedCommitsLen {
		t.Fatalf("List commits len=%d, expected=%d", len(commits), expectedCommitsLen)
	}

	// create a branch and sync change before revert
	_, err = c.CreateBranch(ctx, repository, "branch1", "master")
	testutil.MustDo(t, "create branch1", err)
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "fileX", nil, "")
	_, err = c.Commit(ctx, repository, "master", "commit file x", "tester", nil)
	testutil.MustDo(t, "commit file x", err)
	_, err = c.Merge(ctx, repository, "master", "branch1", "tester", "sync file x", nil)
	testutil.MustDo(t, "merge master to branch1", err)

	// try to rollback to first commit should fail - branch1 blocks it
	err = c.RollbackCommit(ctx, repository, refs[0])
	if err == nil {
		t.Fatal("Rollback with blocked branch should fail with error")
	}
}
