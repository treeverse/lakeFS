package catalog

import (
	"context"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_Merge_FromFather(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// create 3 files on master and commit
	for i := 0; i < 3; i++ {
		testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file"+strconv.Itoa(i), nil, "")
	}
	_, err := c.Commit(ctx, repository, "master", "commit to master", "tester", nil)
	testutil.MustDo(t, "commit to master", err)

	// create branch based on master
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")

	// add new file
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file5", nil, "")

	// delete committed file
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", "/file1"))

	// change/override committed file
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file2", nil, "seed1")

	// commit, merge and verify
	_, err = c.Commit(ctx, repository, "master", "second commit to master", "tester", nil)
	testutil.MustDo(t, "second commit to master", err)

	// merge master to branch1
	res, err := c.Merge(ctx, repository, "master", "branch1", "tester", nil)
	if err != nil {
		t.Fatal("Merge from master to branch1 failed:", err)
	}
	if res.CommitID <= 0 {
		t.Fatalf("Merge commit ID = %d, expected new commit ID", res.CommitID)
	}

	// verify that new file was merged into the branch
	ent, err := c.GetEntry(ctx, repository, "branch1", CommittedID, "/file5")
	if err != nil {
		t.Fatal("Get new entry after merge failed:", err)
	}
	const file5Addr = "53c9486452c01e26833296dcf1f701379fa22f01e610dd9817d064093daab07d"
	if ent.PhysicalAddress != file5Addr {
		t.Fatalf("Get new entry address = %s, expected %s", ent.PhysicalAddress, file5Addr)
	}
	//expectedDiffLen := 1
	//if len(res.Differences) != expectedDiffLen {
	//	t.Fatalf("Merge differences len = %d, expected %d", len(res.Differences), expectedDiffLen)
	//}
}
