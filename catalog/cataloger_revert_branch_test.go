package catalog

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

func TestCataloger_RevertBranch_NoChanges(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	err := c.RevertBranch(ctx, repository, "master")
	if err != nil {
		t.Fatal("Revert branch should work on empty branch")
	}
}

func TestCataloger_RevertBranch_ChangesOnBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	// commit data
	for i := 0; i < 3; i++ {
		if err := c.CreateEntry(ctx, repository, "master", "/file"+strconv.Itoa(i), strings.Repeat("01", i+1), "/addr"+strconv.Itoa(i), i+1, nil); err != nil {
			t.Fatal("create entry for RevertBranch:", err)
		}
	}
	_, err := c.Commit(ctx, repository, "master", "commit three files", "tester", nil)
	if err != nil {
		t.Fatal("commit for RevertBranch:", err)
	}

	// do some changes
	if err := c.DeleteEntry(ctx, repository, "master", "/file1"); err != nil {
		t.Fatal("delete for RevertBranch:", err)
	}
	for i := 3; i < 6; i++ {
		if err := c.CreateEntry(ctx, repository, "master", "/file"+strconv.Itoa(i), strings.Repeat("01", i+1), "/addr"+strconv.Itoa(i), i+1, nil); err != nil {
			t.Fatal("create entry for RevertBranch:", err)
		}
	}

	if err := c.RevertBranch(ctx, repository, "master"); err != nil {
		t.Fatal("Revert branch should work on empty branch")
	}
	entries, _, err := c.ListEntries(ctx, repository, "master", "", "", -1, true)
	if err != nil {
		t.Fatal("ListEntries for RevertBranch test:", err)
	}
	expectedEntriesLen := 3
	if len(entries) != expectedEntriesLen {
		t.Fatalf("ListEntries for RevertBranch should return %d items, got %d", expectedEntriesLen, len(entries))
	}
}

func TestCataloger_RevertBranch_ChangesOnParent(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	// commit data
	for i := 0; i < 3; i++ {
		if err := c.CreateEntry(ctx, repository, "master", "/file"+strconv.Itoa(i), strings.Repeat("01", i+1), "/addr"+strconv.Itoa(i), i+1, nil); err != nil {
			t.Fatal("create entry for RevertBranch:", err)
		}
	}
	_, err := c.Commit(ctx, repository, "master", "commit three files", "tester", nil)
	if err != nil {
		t.Fatal("commit for RevertBranch:", err)
	}

	// create branch
	if _, err := c.CreateBranch(ctx, repository, "b1", "master"); err != nil {
		t.Fatal("CreateBrach for RevertBranch:", err)
	}

	// do some changes
	if err := c.DeleteEntry(ctx, repository, "b1", "/file1"); err != nil {
		t.Fatal("delete for RevertBranch:", err)
	}
	for i := 3; i < 6; i++ {
		if err := c.CreateEntry(ctx, repository, "b1", "/file"+strconv.Itoa(i), strings.Repeat("01", i+1), "/addr"+strconv.Itoa(i), i+1, nil); err != nil {
			t.Fatal("create entry for RevertBranch:", err)
		}
	}

	if err := c.RevertBranch(ctx, repository, "b1"); err != nil {
		t.Fatal("Revert branch should work on empty branch")
	}
	entries, _, err := c.ListEntries(ctx, repository, "b1", "", "", -1, true)
	if err != nil {
		t.Fatal("ListEntries for RevertBranch test:", err)
	}
	expectedEntriesLen := 3
	if len(entries) != expectedEntriesLen {
		t.Fatalf("ListEntries for RevertBranch should return %d items, got %d", expectedEntriesLen, len(entries))
	}
}
