package catalog

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/db"

	"github.com/davecgh/go-spew/spew"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_Merge_FromFatherThreeBranchesExtended1(t *testing.T) {
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

	// make changes on master
	const newFilename = "/file555"
	testCatalogerCreateEntry(t, ctx, c, repository, "master", newFilename, nil, "")
	const delFilename = "/file1"
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", delFilename))
	const overFilename = "/file2"
	testCatalogerCreateEntry(t, ctx, c, repository, "master", overFilename, nil, "seed1")
	_, err := c.Commit(ctx, repository, "master", "second commit to master", "tester", nil)
	testutil.MustDo(t, "second commit to master", err)

	// merge the above down (from master) to branch1
	_, err = c.Merge(ctx, repository, "master", "branch1", "tester", "", nil)
	testutil.MustDo(t, "Merge changes from master to branch1", err)
	// merge the changes from branch1 to branch2
	res, err := c.Merge(ctx, repository, "branch1", "branch2", "tester", "", nil)
	testutil.MustDo(t, "Merge changes from master to branch1", err)

	// verify valid commit id
	if !IsValidReference(res.Reference) {
		t.Errorf("Merge reference = %s, expected a valid reference", res.Reference)
	}
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
		Difference{Type: DifferenceTypeChanged, Path: "/file2"},
		Difference{Type: DifferenceTypeAdded, Path: "/file555"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}

	testVerifyEntries(t, ctx, c, repository, "branch2", []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
	})
	// test that an object deleted at master becomes deleted at branch2 only after merges from both fathers
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", "/file0"))
	_, err = c.Commit(ctx, repository, "master", "commit file0 deletion", "tester", nil)

	getEntryTest(c, t, ctx, repository, "branch2", "/file0", true)
	getEntryTest(c, t, ctx, repository, "branch1", "/file0", true)
	getEntryTest(c, t, ctx, repository, "master", "/file0", false)

	_, err = c.Merge(ctx, repository, "master", "branch1", "tester", "", nil)
	if err != nil {
		t.Errorf("error in merge %w", err)
	}
	getEntryTest(c, t, ctx, repository, "branch2", "/file0", true)
	getEntryTest(c, t, ctx, repository, "branch1", "/file0", false)
	getEntryTest(c, t, ctx, repository, "master", "/file0", false)

	_, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "", nil)
	if err != nil {
		t.Errorf("error in merge %w", err)
	}
	getEntryTest(c, t, ctx, repository, "branch2", "/file0", false)
	getEntryTest(c, t, ctx, repository, "branch1", "/file0", false)
	getEntryTest(c, t, ctx, repository, "master", "/file0", false)

	// test that the same object with the same name does not create a conflict in son to father , and is not a change

	testCatalogerCreateEntry(t, ctx, c, repository, "branch2", "/file0", nil, "seed1")
	_, _ = c.Commit(ctx, repository, "branch2", "commit file0 creation", "tester", nil)
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file0", nil, "seed1")
	_, _ = c.Commit(ctx, repository, "master", "commit file0 creation", "tester", nil)
	res, err = c.Merge(ctx, repository, "master", "branch1", "tester", "", nil)
	expectedDifferences = Differences{
		Difference{Type: DifferenceTypeAdded, Path: "/file0"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
	res, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "", nil)

	if len(res.Differences) != 0 {
		t.Errorf("unexpected Merge differences = %s", spew.Sdump(res.Differences))
	}

	// deletion in master will force  physically delete in grandson
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", "/file0"))
	_, err = c.Commit(ctx, repository, "master", "commit file0 deletion", "tester", nil)
	res, err = c.Merge(ctx, repository, "master", "branch1", "tester", "bubling /file0 deletion up", nil)
	res, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "forcing file0 on branch2 to delete", nil)
	expectedDifferences = Differences{
		Difference{Type: DifferenceTypeRemoved, Path: "/file0"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
	//identical entries created in son and grandfather do not create conflict - even when grandfather is uncommitted
	res, err = c.Merge(ctx, repository, "branch2", "branch1", "tester", "pempty updates", nil)
	res, err = c.Merge(ctx, repository, "branch1", "master", "tester", "empty updates", nil)
	testCatalogerCreateEntry(t, ctx, c, repository, "branch2", "/file111", nil, "seed1")
	_, _ = c.Commit(ctx, repository, "branch2", "commit file0 creation", "tester", nil)
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file111", nil, "seed1")
	res, err = c.Merge(ctx, repository, "branch2", "branch1", "tester", "pushing /file111 down", nil)
	res, err = c.Merge(ctx, repository, "branch1", "master", "tester", "pushing /file111 down", nil)
	// push file111 delete
	res, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "delete /file111 up", nil)
	testutil.MustDo(t, "delete committed file on branch1",
		c.DeleteEntry(ctx, repository, "branch1", "/file111"))
	_, err = c.Commit(ctx, repository, "branch1", "commit file111 deletion", "tester", nil)

	res, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "delete /file111 up", nil)
	expectedDifferences = Differences{
		Difference{Type: DifferenceTypeRemoved, Path: "/file111"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
	res, err = c.Merge(ctx, repository, "branch1", "master", "tester", "try delete /file111 . get conflict", nil)

	expectedDifferences = Differences{
		Difference{Type: DifferenceTypeConflict, Path: "/file111"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}

}

func getEntryTest(c Cataloger, t *testing.T, ctx context.Context, repository, reference string, path string, expect bool) {
	entry, err := c.GetEntry(ctx, repository, reference, path, GetEntryParams{ReturnExpired: true})
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		t.Errorf("error in get entry %s branch %s", path, reference)
	}
	if expect && entry == nil {
		t.Errorf("%s on branch %s expected not found", path, reference)
	} else if !expect && entry != nil {
		t.Errorf("%s on branch %s not expected BUT found", path, reference)
	}

}
