package mvcc

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/catalog"
)

func TestCataloger_ResetBranch_NoChanges(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	err := c.ResetBranch(ctx, repository, "master")
	if err != nil {
		t.Fatal("Reset branch should work on empty branch")
	}
}

func TestCataloger_ResetBranch_ChangesOnBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	// commit data
	for i := 0; i < 3; i++ {
		if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
			Path:            "/file" + strconv.Itoa(i),
			Checksum:        strings.Repeat("01", i+1),
			PhysicalAddress: "/addr" + strconv.Itoa(i),
			Size:            int64(i) + 1,
		}, catalog.CreateEntryParams{}); err != nil {
			t.Fatal("create entry for ResetBranch:", err)
		}
	}
	_, err := c.Commit(ctx, repository, "master", "commit three files", "tester", nil)
	if err != nil {
		t.Fatal("Commit for ResetBranch:", err)
	}

	// do some changes
	if err := c.DeleteEntry(ctx, repository, "master", "/file1"); err != nil {
		t.Fatal("delete for ResetBranch:", err)
	}
	for i := 3; i < 6; i++ {
		if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
			Path:            "/file" + strconv.Itoa(i),
			Checksum:        strings.Repeat("01", i+1),
			PhysicalAddress: "/addr" + strconv.Itoa(i),
			Size:            int64(i) + 1,
		}, catalog.CreateEntryParams{}); err != nil {
			t.Fatal("create entry for ResetBranch:", err)
		}
	}

	if err := c.ResetBranch(ctx, repository, "master"); err != nil {
		t.Fatal("Reset branch should work on empty branch")
	}
	reference := catalog.MakeReference("master", catalog.UncommittedID)
	entries, _, err := c.ListEntries(ctx, repository, reference, "", "", "", -1)
	if err != nil {
		t.Fatal("ListEntries for ResetBranch test:", err)
	}
	expectedEntriesLen := 3
	if len(entries) != expectedEntriesLen {
		t.Fatalf("ListEntries for ResetBranch should return %d items, got %d", expectedEntriesLen, len(entries))
	}
}

func TestCataloger_ResetBranch_ChangesOnParent(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")

	// commit data
	for i := 0; i < 3; i++ {
		if err := c.CreateEntry(ctx, repository, "master", catalog.Entry{
			Path:            "/file" + strconv.Itoa(i),
			Checksum:        strings.Repeat("01", i+1),
			PhysicalAddress: "/addr" + strconv.Itoa(i),
			Size:            int64(i) + 1,
		}, catalog.CreateEntryParams{}); err != nil {
			t.Fatal("create entry for ResetBranch:", err)
		}
	}
	_, err := c.Commit(ctx, repository, "master", "commit three files", "tester", nil)
	if err != nil {
		t.Fatal("Commit for ResetBranch:", err)
	}

	// create branch
	if _, err := c.CreateBranch(ctx, repository, "b1", "master"); err != nil {
		t.Fatal("CreateBranch for ResetBranch:", err)
	}

	// do some changes
	if err := c.DeleteEntry(ctx, repository, "b1", "/file1"); err != nil {
		t.Fatal("delete for ResetBranch:", err)
	}
	for i := 3; i < 6; i++ {
		if err := c.CreateEntry(ctx, repository, "b1", catalog.Entry{
			Path:            "/file" + strconv.Itoa(i),
			Checksum:        strings.Repeat("01", i+1),
			PhysicalAddress: "/addr" + strconv.Itoa(i),
			Size:            int64(i) + 1,
			Metadata:        nil,
		}, catalog.CreateEntryParams{}); err != nil {
			t.Fatal("create entry for ResetBranch:", err)
		}
	}

	if err := c.ResetBranch(ctx, repository, "b1"); err != nil {
		t.Fatal("Reset branch should work on empty branch")
	}
	entries, _, err := c.ListEntries(ctx, repository, catalog.MakeReference("b1", catalog.UncommittedID), "", "", "", -1)
	if err != nil {
		t.Fatal("ListEntries for ResetBranch test:", err)
	}
	expectedEntriesLen := 3
	if len(entries) != expectedEntriesLen {
		t.Fatalf("ListEntries for ResetBranch should return %d items, got %d", expectedEntriesLen, len(entries))
	}
}
