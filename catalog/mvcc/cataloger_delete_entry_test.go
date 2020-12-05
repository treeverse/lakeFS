package mvcc

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/testutil"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

func TestCataloger_DeleteEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	t.Run("delete file not exists", func(t *testing.T) {
		err := c.DeleteEntry(ctx, repository, "master", "/file1")
		wantErr := catalog.ErrEntryNotFound
		if !errors.As(err, &wantErr) {
			t.Errorf("DeleteEntry() error = %s, want = %s", err, wantErr)
		}
	})

	t.Run("delete uncommitted", func(t *testing.T) {
		if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
			Path:            "/file2",
			Checksum:        "ff",
			PhysicalAddress: "/addr2",
			Size:            2,
			Metadata:        nil,
		}, catalog.CreateEntryParams{}); err != nil {
			t.Fatal("create entry for delete entry test:", err)
		}
		err := c.DeleteEntry(ctx, repository, "master", "/file2")
		if err != nil {
			t.Errorf("DeleteEntry() error = %s, expected no error", err)
			return
		}

		testDeleteEntryExpectNotFound(t, ctx, c, repository, "master", "/file2")

		// if we try to commit we should fail - there was no change
		_, err = c.Commit(ctx, repository, "master", "commit nothing", "tester", nil)
		if !errors.Is(err, catalog.ErrNothingToCommit) {
			t.Fatalf("Commit returned err=%s, expected=%s", err, catalog.ErrNothingToCommit)
		}
	})

	t.Run("delete committed on branch", func(t *testing.T) {
		if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
			Path:            "/file3",
			Checksum:        "ffff",
			PhysicalAddress: "/addr3",
			Size:            2,
			Metadata:        nil,
		}, catalog.CreateEntryParams{}); err != nil {
			t.Fatal("create entry for delete entry test:", err)
		}
		if _, err := c.Commit(ctx, repository, "master", "commit file3", "tester", nil); err != nil {
			t.Fatal("Commit entry for delete entry test:", err)
		}
		err := c.DeleteEntry(ctx, repository, "master", "/file3")
		if err != nil {
			t.Errorf("DeleteEntry() error = %s, want no error", err)
			return
		}
		testDeleteEntryExpectNotFound(t, ctx, c, repository, "master", "/file3")
		testDeleteEntryCommitAndExpectNotFound(t, ctx, c, repository, "master", "/file3")
	})

	t.Run("delete file committed on parent", func(t *testing.T) {
		if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
			Path:            "/file4",
			Checksum:        "ffff",
			PhysicalAddress: "/addr4",
			Size:            4,
			Metadata:        nil,
		}, catalog.CreateEntryParams{}); err != nil {
			t.Fatal("create entry for delete entry test:", err)
		}
		if _, err := c.Commit(ctx, repository, "master", "commit file4", "tester", nil); err != nil {
			t.Fatal("Commit entry for delete entry test:", err)
		}
		if _, err := c.CreateBranch(ctx, repository, "b1", "master"); err != nil {
			t.Fatal("create branch for delete entry test:", err)
		}
		err := c.DeleteEntry(ctx, repository, "b1", "/file4")
		if err != nil {
			t.Errorf("DeleteEntry() error = %s, want no error", err)
			return
		}
		testDeleteEntryExpectNotFound(t, ctx, c, repository, "b1", "/file4")
		testDeleteEntryCommitAndExpectNotFound(t, ctx, c, repository, "b1", "/file4")
	})
}

func testDeleteEntryExpectNotFound(t *testing.T, ctx context.Context, c catalog.Cataloger, repository, branch string, path string) {
	_, err := c.GetEntry(ctx, repository, MakeReference(branch, UncommittedID), path, catalog.GetEntryParams{})
	wantErr := db.ErrNotFound
	if !errors.As(err, &wantErr) {
		t.Fatalf("DeleteEntry() get entry err = %s, want = %s", err, wantErr)
	}
	// expect a second delete to fail on entry not found
	err = c.DeleteEntry(ctx, repository, branch, path)
	wantErr = catalog.ErrEntryNotFound
	if !errors.As(err, &wantErr) {
		t.Fatalf("DeleteEntry() error = %s, want = %s", err, wantErr)
	}
}

func testDeleteEntryCommitAndExpectNotFound(t *testing.T, ctx context.Context, c catalog.Cataloger, repository, branch string, path string) {
	_, err := c.Commit(ctx, repository, branch, "commit before expect not found "+path, "tester", nil)
	if err != nil {
		t.Fatal("Failed to commit before expect not found:", err)
	}
	_, err = c.GetEntry(ctx, repository, branch+CommittedSuffix, path, catalog.GetEntryParams{})
	wantErr := db.ErrNotFound
	if !errors.As(err, &wantErr) {
		t.Fatalf("DeleteEntry() get entry err = %s, want = %s", err, wantErr)
	}
}

func TestCataloger_DeleteEntryAndCheckItRemainsInCommits(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
		Path:            "/file2",
		Checksum:        "ff",
		PhysicalAddress: "/addr2",
		Size:            2,
		Metadata:        nil,
	}, catalog.CreateEntryParams{}); err != nil {
		t.Fatal("create entry for delete entry test:", err)
	}
	prevCommit, err := c.Commit(ctx, repository, "master", "commit before deletion test failed ", "tester", nil)
	if err != nil {
		t.Fatal("Failed to commit before expect not found:", err)
	}
	entry, err := c.GetEntry(ctx, repository, prevCommit.Reference, "/file2", catalog.GetEntryParams{})
	_ = entry
	err = c.DeleteEntry(ctx, repository, "master", "/file2")
	if err != nil {
		t.Fatal("delete failed: ", err)
	}
	nextCommit, err := c.Commit(ctx, repository, "master", "commit after deletion ", "tester", nil)
	if err != nil {
		t.Fatal("Failed to commit after delete:", err)
	}
	entry, err = c.GetEntry(ctx, repository, nextCommit.Reference, "/file2", catalog.GetEntryParams{})
	entry, err = c.GetEntry(ctx, repository, prevCommit.Reference, "/file2", catalog.GetEntryParams{})
	list, _, err := c.ListEntries(ctx, repository, nextCommit.Reference, "", "", "", 1000)
	if len(list) != 0 {
		t.Fatal("list entries returned deleted object")
	}
	list, _, err = c.ListEntries(ctx, repository, prevCommit.Reference, "", "", "", 1000)
	if len(list) != 1 {
		t.Fatal("list entries by commitID did not return deleted object from next commit")
	}
	list, _, err = c.ListEntries(ctx, repository, nextCommit.Reference, "", "", "/", 1000)
	if len(list) != 0 {
		t.Fatal("list entries by prefix returned deleted object")
	}
	list, _, err = c.ListEntries(ctx, repository, prevCommit.Reference, "", "", "/", 1000)
	if len(list) != 1 {
		t.Fatal("list entries by prefix on commitID did not return deleted object from next commit")
	}
}

func TestCataloger_DeleteEntryVerifyExisting(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	testCatalogerCreateEntry(t, ctx, c, repository, "master", "file1", nil, "")
	commit1, err := c.Commit(ctx, repository, "master", "add file1", "committer", nil)
	testutil.MustDo(t, "commit add file1", err)

	_, err = c.CreateBranch(ctx, repository, "branch1", "master")
	testutil.MustDo(t, "create branch1", err)

	err = c.DeleteEntry(ctx, repository, "master", "file1")
	testutil.MustDo(t, "delete file1", err)

	_, err = c.Commit(ctx, repository, "master", "delete file1", "committer", nil)
	testutil.MustDo(t, "commit delete file1", err)

	// check file exists using reference, branch and listing
	_, err = c.GetEntry(ctx, repository, commit1.Reference, "file1", catalog.GetEntryParams{})
	testutil.MustDo(t, "get file1 by ref", err)

	_, err = c.GetEntry(ctx, repository, "branch1", "file1", catalog.GetEntryParams{})
	testutil.MustDo(t, "get file1 by branch", err)

	entriesRef, _, err := c.ListEntries(ctx, repository, commit1.Reference, "", "", "", -1)
	testutil.MustDo(t, "list using ref", err)
	if len(entriesRef) != 1 {
		t.Error("ListEntries of ref before delete should include a file")
	}

	entriesBranch, _, err := c.ListEntries(ctx, repository, "branch1", "", "", "", -1)
	testutil.MustDo(t, "list using branch1", err)
	if len(entriesBranch) != 1 {
		t.Error("ListEntries of branch before delete should include a file")
	}

	// check the file is deleted on master
	_, err = c.GetEntry(ctx, repository, "master", "file1", catalog.GetEntryParams{})
	if !errors.Is(err, catalog.ErrEntryNotFound) {
		t.Error("GetEntry should return not found on master branch:", err)
	}
}
