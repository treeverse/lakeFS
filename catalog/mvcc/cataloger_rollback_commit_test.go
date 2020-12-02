package mvcc

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_RollbackCommit_Basic(t *testing.T) {
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
		err := c.RollbackCommit(ctx, repository, "master", ref)
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
}

func TestCataloger_RollbackCommit_BlockedByBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// get first commit
	masterReference, err := c.GetBranchReference(ctx, repository, "master")
	testutil.MustDo(t, "getting master branch reference", err)

	// create a branch
	_, err = c.CreateBranch(ctx, repository, "branch1", "master")
	testutil.MustDo(t, "create branch1", err)

	// commit new data to master
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "fileX", nil, "")
	_, err = c.Commit(ctx, repository, "master", "commit file x", "tester", nil)
	testutil.MustDo(t, "commit file x", err)

	// merge changes into the branch1
	_, err = c.Merge(ctx, repository, "master", "branch1", "tester", "sync file x", nil)
	testutil.MustDo(t, "merge master to branch1", err)

	// rollback to initial commit should fail
	err = c.RollbackCommit(ctx, repository, "master", masterReference)
	if err == nil {
		t.Fatal("Rollback with blocked branch should fail with error")
	}
}

func TestCataloger_RollbackCommit_AfterMerge(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// create and commit a file - fileFile
	filenames := []string{"file1", "file2"}
	for _, filename := range filenames {
		testCatalogerCreateEntry(t, ctx, c, repository, "master", filename, nil, "")
	}
	firstCommit, err := c.Commit(ctx, repository, "master", "first file", "tester", nil)
	testutil.MustDo(t, "first commit", err)

	// create a branch and commit some changes - add, delete, modify and commit
	_, err = c.CreateBranch(ctx, repository, "branch1", "master")
	testutil.MustDo(t, "create branch1", err)
	// update file1 on branch1
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "file1", nil, "branch1")
	// delete file2 on branch1
	err = c.DeleteEntry(ctx, repository, "branch1", "file2")
	testutil.MustDo(t, "delete file2", err)
	// add file2 on branch1
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "file2", nil, "branch1")
	// commit changes
	_, err = c.Commit(ctx, repository, "branch1", "tester", "changes", nil)
	testutil.MustDo(t, "commit changes to branch1", err)

	// merge changes from branch1 to master
	_, err = c.Merge(ctx, repository, "branch1", "master", "tester", "sync branch1 to master", nil)
	testutil.MustDo(t, "merge branch1 to master", err)

	// rollback to first commit
	err = c.RollbackCommit(ctx, repository, "master", firstCommit.Reference)
	testutil.MustDo(t, "rollback to first commit", err)

	// check we have our original files
	for _, filename := range filenames {
		ent, err := c.GetEntry(ctx, repository, "master", filename, catalog.GetEntryParams{})
		testutil.MustDo(t, filename+" get should work", err)

		expectedChecksum := testCreateEntryCalcChecksum(filename, t.Name(), "")
		if expectedChecksum != ent.Checksum {
			t.Fatalf("Value file1 after revert checksum %s, expected %s", ent.Checksum, expectedChecksum)
		}
	}
}
