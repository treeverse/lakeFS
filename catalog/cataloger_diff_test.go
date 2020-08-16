package catalog

import (
	"context"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_Diff_FromChildThreeBranches(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// create 3 files and commit
	commitThreeFiles := func(msg, branch string, offset int) {
		const items = 3
		for i := 0; i < items; i++ {
			testCatalogerCreateEntry(t, ctx, c, repository, branch, "/file"+strconv.Itoa(i+items*offset), nil, "")
		}
		_, err := c.Commit(ctx, repository, branch, msg, "tester", nil)
		testutil.MustDo(t, msg, err)
	}
	// create 3 files and commit on master
	commitThreeFiles("First commit to master", "master", 0)

	// create branch1 based on master with 3 files committed
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")
	commitThreeFiles("First commit to branch1", "branch1", 1)

	// create branch2 based on branch1 with 3 files committed
	testCatalogerBranch(t, ctx, c, repository, "branch2", "branch1")
	commitThreeFiles("First commit to branch2", "branch2", 2)

	// make changes on branch2
	const newFilename = "/file555"
	testCatalogerCreateEntry(t, ctx, c, repository, "branch2", newFilename, nil, "")
	const delFilename = "/file1"
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "branch2", delFilename))
	const overFilename = "/file2"
	testCatalogerCreateEntry(t, ctx, c, repository, "branch2", overFilename, nil, "seed1")
	_, err := c.Commit(ctx, repository, "branch2", "second commit to branch2", "tester", nil)
	testutil.MustDo(t, "second commit to branch2", err)

	// merge the above up to master (from branch2)
	_, err = c.Merge(ctx, repository, "branch2", "branch1", "tester", "", nil)
	testutil.MustDo(t, "Merge changes from branch2 to branch1", err)
	// merge the changes from branch1 to master
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", "", nil)
	testutil.MustDo(t, "Merge changes from branch1 to master", err)

	if !IsValidReference(res.Reference) {
		t.Errorf("Merge reference = %s, expected a valid reference", res.Reference)
	}
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeChanged, Path: "/file2"},
		Difference{Type: DifferenceTypeAdded, Path: "/file3"},
		Difference{Type: DifferenceTypeAdded, Path: "/file4"},
		Difference{Type: DifferenceTypeAdded, Path: "/file5"},
		Difference{Type: DifferenceTypeAdded, Path: "/file555"},
		Difference{Type: DifferenceTypeAdded, Path: "/file6"},
		Difference{Type: DifferenceTypeAdded, Path: "/file7"},
		Difference{Type: DifferenceTypeAdded, Path: "/file8"},
		Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}

	testVerifyEntries(t, ctx, c, repository, "master:HEAD", []testEntryInfo{
		{Path: "/file1", Deleted: true},
		{Path: "/file2", Seed: "seed1"},
		{Path: "/file3"},
		{Path: "/file4"},
		{Path: "/file555"},
		{Path: "/file6"},
		{Path: "/file7"},
		{Path: "/file8"},
	})
}
