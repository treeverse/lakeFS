package catalog

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"

	"github.com/treeverse/lakefs/db"

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
	const newFilename = "/file5"
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file5", nil, "")

	// delete committed file
	const delFilename = "/file1"
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", delFilename))

	// change/override committed file
	const overFilename = "/file2"
	testCatalogerCreateEntry(t, ctx, c, repository, "master", overFilename, nil, "seed1")

	// commit, merge and verify
	_, err = c.Commit(ctx, repository, "master", "second commit to master", "tester", nil)
	testutil.MustDo(t, "second commit to master", err)

	// before the merge - make sure we see the deleted file
	_, err = c.GetEntry(ctx, repository, "branch1", CommittedID, delFilename)
	if err != nil {
		t.Fatalf("Get entry %s, expected to be found: %s", delFilename, err)
	}

	// merge master to branch1
	res, err := c.Merge(ctx, repository, "master", "branch1", "tester", nil)
	if err != nil {
		t.Fatal("Merge from master to branch1 failed:", err)
	}
	if res.CommitID <= 0 {
		t.Fatalf("Merge commit ID = %d, expected new commit ID", res.CommitID)
	}

	// verify that new file was merged into the branch
	ent, err := c.GetEntry(ctx, repository, "branch1", CommittedID, newFilename)
	if err != nil {
		t.Fatalf("Get entry %s after merge failed: %s", newFilename, err)
	}
	faddr := "53c9486452c01e26833296dcf1f701379fa22f01e610dd9817d064093daab07d"
	if ent.PhysicalAddress != faddr {
		t.Fatalf("Get entry %s address = %s, expected %s", newFilename, ent.PhysicalAddress, faddr)
	}

	// verify that the change we made was merged into the branch
	ent, err = c.GetEntry(ctx, repository, "branch1", CommittedID, overFilename)
	if err != nil {
		t.Fatalf("Get entry %s after merge failed: %s", overFilename, err)
	}
	faddr = "63564f8303a4c07a145057f7401b64d58c22485f07fcfbe1270539142b1709d3"
	if ent.PhysicalAddress != faddr {
		t.Fatalf("Get entry %s address = %s, expected %s", overFilename, ent.PhysicalAddress, faddr)
	}

	// verify that we no longer see the deleted file
	ent, err = c.GetEntry(ctx, repository, "branch1", CommittedID, delFilename)
	if !errors.As(err, &db.ErrNotFound) {
		t.Fatalf("Get entry %s after merge expected not to be found: %s", delFilename, err)
	}

	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeChanged, Path: "/file2"},
		Difference{Type: DifferenceTypeAdded, Path: "/file5"},
		Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	}
	if reflect.DeepEqual(res.Differences, expectedDifferences) {
		t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}

}
