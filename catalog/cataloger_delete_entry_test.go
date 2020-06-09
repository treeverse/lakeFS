package catalog

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/db"
)

func TestCataloger_DeleteEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	t.Run("delete file not exists", func(t *testing.T) {
		err := c.DeleteEntry(ctx, repository, "master", "/file1")
		wantErr := ErrEntryNotFound
		if !errors.As(err, &wantErr) {
			t.Errorf("DeleteEntry() error = %s, want = %s", err, wantErr)
		}
	})

	t.Run("delete uncommitted", func(t *testing.T) {
		if err := c.CreateEntry(ctx, repository, "master", "/file2", "ff", "/addr2", 2, nil); err != nil {
			t.Fatal("create entry for delete entry test:", err)
		}
		err := c.DeleteEntry(ctx, repository, "master", "/file2")
		if err != nil {
			t.Errorf("DeleteEntry() error = %s, expected no error", err)
			return
		}
		// expect not to find the file
		testDeleteEntryExpectNotFound(t, ctx, c, repository, "master", "/file2")
	})

	t.Run("delete committed on branch", func(t *testing.T) {
		if err := c.CreateEntry(ctx, repository, "master", "/file3", "ffff", "/addr3", 2, nil); err != nil {
			t.Fatal("create entry for delete entry test:", err)
		}
		if _, err := c.Commit(ctx, repository, "master", "commit delete test", "tester", nil); err != nil {
			t.Fatal("commit entry for delete entry test:", err)
		}
		err := c.DeleteEntry(ctx, repository, "master", "/file3")
		if err != nil {
			t.Errorf("DeleteEntry() error = %s, want no error", err)
			return
		}
		testDeleteEntryExpectNotFound(t, ctx, c, repository, "master", "/file3")
	})

	t.Run("delete file committed on parent", func(t *testing.T) {
		if err := c.CreateEntry(ctx, repository, "master", "/file4", "ffff", "/addr4", 4, nil); err != nil {
			t.Fatal("create entry for delete entry test:", err)
		}
		if _, err := c.Commit(ctx, repository, "master", "commit file4", "tester", nil); err != nil {
			t.Fatal("commit entry for delete entry test:", err)
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
		// TODO(barak): call commit and check that the file is deleted
	})
}

func testDeleteEntryExpectNotFound(t *testing.T, ctx context.Context, c Cataloger, repository string, branch string, path string) {
	_, err := c.GetEntry(ctx, repository, branch, path, true)
	wantErr := db.ErrNotFound
	if !errors.As(err, &wantErr) {
		t.Fatalf("DeleteEntry() get entry err = %s, want = %s", err, wantErr)
	}
	// expect a second delete to fail on entry not found
	err = c.DeleteEntry(ctx, repository, branch, path)
	wantErr = ErrEntryNotFound
	if !errors.As(err, &wantErr) {
		t.Fatalf("DeleteEntry() error = %s, want = %s", err, wantErr)
	}
}
