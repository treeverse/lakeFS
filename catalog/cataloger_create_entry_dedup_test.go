package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_CreateEntryDedup(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	defer func() { _ = c.Close() }()

	const testBranch = "master"
	repo := testCatalogerRepo(t, ctx, c, "repo", testBranch)

	const firstAddr = "1"
	const secondAddr = "2"
	// add first entry
	ent1 := Entry{
		Path:            "file1",
		PhysicalAddress: firstAddr,
		CreationDate:    time.Now(),
		Size:            0,
		Checksum:        "aa",
	}
	dedup1 := DedupParams{
		ID:               "aa",
		StorageNamespace: "s1",
	}
	testutil.MustDo(t, "create first entry",
		c.CreateEntryDedup(ctx, repo, testBranch, ent1, dedup1))

	// add second entry with the same dedup id
	dedup2 := DedupParams{
		ID:               "aa",
		StorageNamespace: "s2",
	}
	ent2 := Entry{
		Path:            "file2",
		PhysicalAddress: secondAddr,
		CreationDate:    time.Now(),
		Size:            0,
		Checksum:        "aa",
	}
	testutil.MustDo(t, "create second entry, same content",
		c.CreateEntryDedup(ctx, repo, testBranch, ent2, dedup2))
	select {
	case report := <-c.DedupReportChannel():
		if report.Entry.Path != "file2" && report.NewPhysicalAddress == "" {
			t.Fatal("second entry should have new physical address after dedup")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for dedup report")
	}
}
