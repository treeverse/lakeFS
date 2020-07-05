package catalog

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"

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
	dedupCh := make(chan *DedupResult)
	ent1 := Entry{
		Path:            "file1",
		PhysicalAddress: firstAddr,
		CreationDate:    time.Now(),
		Size:            0,
		Checksum:        "aa",
	}
	testutil.MustDo(t, "create first entry no dups",
		c.CreateEntryDedup(ctx, repo, testBranch, ent1, "aa", dedupCh))
	res1 := <-dedupCh
	if !reflect.DeepEqual(*res1.Entry, ent1) {
		t.Errorf("Dedup entry = %s, expected %s", spew.Sdump(*res1.Entry), spew.Sdump(ent1))
	}
	if res1.NewPhysicalAddress != "" {
		t.Fatalf("Dedup new address: %s, expected none", res1.NewPhysicalAddress)
	}

	// add second entry with the same dedup id
	ent2 := Entry{
		Path:            "file2",
		PhysicalAddress: secondAddr,
		CreationDate:    time.Now(),
		Size:            0,
		Checksum:        "aa",
	}
	testutil.MustDo(t, "create first entry no dups",
		c.CreateEntryDedup(ctx, repo, testBranch, ent2, "aa", dedupCh))
	res2 := <-dedupCh
	if !reflect.DeepEqual(res2.Entry, &ent2) {
		t.Errorf("Dedup entry = %s, expected %s", spew.Sdump(*res2.Entry), spew.Sdump(ent2))
	}
	if res1.NewPhysicalAddress == firstAddr {
		t.Fatalf("Dedup new address: %s, expected %s", res1.NewPhysicalAddress, firstAddr)
	}
}
