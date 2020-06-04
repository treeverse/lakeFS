package catalog

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/db"
)

func TestCataloger_DeleteEntry(t *testing.T) {
	ctx := context.Background()
	c := setupCatalogerForTesting(t)
	repo := setupCatalogerRepo(t, ctx, c, "repo", "master")

	t.Run("delete file not exists", func(t *testing.T) {
		err := c.DeleteEntry(ctx, repo, "master", "/file1")
		wantErr := ErrEntryNotFound
		if !errors.Is(err, wantErr) {
			t.Errorf("DeleteEntry() error = %s, want = %s", err, wantErr)
		}
	})

	t.Run("delete file uncommitted", func(t *testing.T) {
		if err := c.WriteEntry(ctx, repo, "master", "/file2", "ff", "/addr2", 2, nil); err != nil {
			t.Fatal("write entry for delete entry test:", err)
		}
		err := c.DeleteEntry(ctx, repo, "master", "/file2")
		if err != nil {
			t.Errorf("DeleteEntry() error = %s, want no error", err)
			return
		}
		_, err = c.ReadEntry(ctx, repo, "master", "/file2", true)
		wantErr := db.ErrNotFound
		if !errors.Is(err, wantErr) {
			t.Errorf("DeleteEntry() read entry err = %s, want = %s", err, wantErr)
		}
	})

	t.Run("delete file committed", func(t *testing.T) {
		if err := c.WriteEntry(ctx, repo, "master", "/file3", "ffff", "/addr3", 2, nil); err != nil {
			t.Fatal("write entry for delete entry test:", err)
		}
		if _, err := c.Commit(ctx, repo, "master", "commit delete test", "tester", nil); err != nil {
			t.Fatal("commit entry for delete entry test:", err)
		}
		err := c.DeleteEntry(ctx, repo, "master", "/file3")
		if err != nil {
			t.Errorf("DeleteEntry() error = %s, want no error", err)
			return
		}
		_, err = c.ReadEntry(ctx, repo, "master", "/file3", true)
		wantErr := db.ErrNotFound
		if !errors.Is(err, wantErr) {
			t.Errorf("DeleteEntry() read entry err = %s, want = %s", err, wantErr)
		}
	})
}
