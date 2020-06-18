package catalog

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/davecgh/go-spew/spew"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_Merge_FromFatherNoChangesInChild(t *testing.T) {
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
	testCatalogerCreateEntry(t, ctx, c, repository, "master", newFilename, nil, "")

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

	testVerifyEntries(t, ctx, c, repository, "branch1", CommittedID, []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
	})

	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeChanged, Path: "/file2"},
		Difference{Type: DifferenceTypeAdded, Path: "/file5"},
		Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
}

func TestCataloger_Merge_FromFatherConflicts(t *testing.T) {
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
	testCatalogerCreateEntry(t, ctx, c, repository, "master", newFilename, nil, "")

	// delete committed file
	const delFilename = "/file1"
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", delFilename))

	// change/override committed file
	const overFilename = "/file2"
	testCatalogerCreateEntry(t, ctx, c, repository, "master", overFilename, nil, "seed1")

	// commit changes on master
	_, err = c.Commit(ctx, repository, "master", "second commit to master", "tester", nil)
	testutil.MustDo(t, "second commit to master", err)

	// make other changes to the same files
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "/file5", nil, "seed2")
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "branch1", delFilename))
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", overFilename, nil, "seed2")

	// merge should identify conflicts on pending changes
	res, err := c.Merge(ctx, repository, "master", "branch1", "tester", nil)
	// expected to find 2 conflicts on the files we update/created with the same path
	if !errors.Is(err, ErrConflictFound) {
		t.Errorf("Merge err = %s, expected conflict with err = %s", err, ErrConflictFound)
	}
	if res.CommitID != 0 {
		t.Errorf("Merge commit ID = %d, expected 0", res.CommitID)
	}
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeConflict, Path: "/file2"},
		Difference{Type: DifferenceTypeConflict, Path: "/file5"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
}

func TestCataloger_Merge_FromFatherNoChangesInFather(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")
	res, err := c.Merge(ctx, repository, "master", "branch1", "tester", nil)
	expectedErr := ErrNoDifferenceWasFound
	if !errors.Is(err, expectedErr) {
		t.Errorf("Merge err = %s, expected %s", err, expectedErr)
	}
	if res.CommitID != 0 {
		t.Errorf("Merge commit ID = %d, expected 0", res.CommitID)
	}
	if len(res.Differences) != 0 {
		t.Errorf("Merge differences len=%d, expected 0", len(res.Differences))
	}
}

func TestCataloger_Merge_FromFatherChangesInBoth(t *testing.T) {
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
	testCatalogerCreateEntry(t, ctx, c, repository, "master", newFilename, nil, "")

	// delete committed file
	const delFilename = "/file1"
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", delFilename))

	// change/override committed file
	const overFilename = "/file2"
	testCatalogerCreateEntry(t, ctx, c, repository, "master", overFilename, nil, "seed1")

	// commit changes on master
	_, err = c.Commit(ctx, repository, "master", "second commit to master", "tester", nil)
	testutil.MustDo(t, "second commit to master", err)

	// make other changes
	for i := 0; i < 3; i++ {
		testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "/b2/file"+strconv.Itoa(i), nil, "seed2")
	}
	_, err = c.Commit(ctx, repository, "branch1", "first commit to branch1", "tester", nil)
	testutil.MustDo(t, "first commit on branch1", err)

	// merge should work and grab all the changes from master
	res, err := c.Merge(ctx, repository, "master", "branch1", "tester", nil)
	if err != nil {
		t.Fatal("Merge from master to branch1 failed:", err)
	}
	if res.CommitID <= 0 {
		t.Errorf("Merge commit ID = %d, expected a valid commit number", res.CommitID)
	}
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
		Difference{Type: DifferenceTypeChanged, Path: "/file2"},
		Difference{Type: DifferenceTypeAdded, Path: "/file5"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}

	testVerifyEntries(t, ctx, c, repository, "branch1", CommittedID, []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
		{Path: "/b2/file0", Seed: "seed2"},
		{Path: "/b2/file1", Seed: "seed2"},
		{Path: "/b2/file2", Seed: "seed2"},
	})
}

func TestCataloger_Merge_FromFatherThreeBranches(t *testing.T) {
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
	_, err = c.Merge(ctx, repository, "master", "branch1", "tester", nil)
	testutil.MustDo(t, "Merge changes from master to branch1", err)
	// merge the changes from branch1 to branch2
	res, err := c.Merge(ctx, repository, "branch1", "branch2", "tester", nil)
	testutil.MustDo(t, "Merge changes from master to branch1", err)

	// verify valid commit id
	if res.CommitID <= 0 {
		t.Errorf("Merge commit ID = %d, expected a valid commit number", res.CommitID)
	}
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
		Difference{Type: DifferenceTypeChanged, Path: "/file2"},
		Difference{Type: DifferenceTypeAdded, Path: "/file555"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}

	testVerifyEntries(t, ctx, c, repository, "branch2", CommittedID, []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
	})
}

func TestCataloger_Merge_FromSonNoChanges(t *testing.T) {
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

	// merge empty branch into master
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", nil)
	expectedErr := ErrNoDifferenceWasFound
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Merge from branch1 to master err=%s, expected=%s", err, expectedErr)
	}
	if res.CommitID != 0 {
		t.Fatalf("Merge commit ID = %d, expected 0", res.CommitID)
	}
}

func TestCataloger_Merge_FromSonChangesOnSon(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// create 3 files on master and commit
	for i := 0; i < 3; i++ {
		testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file"+strconv.Itoa(i), nil, "")
	}
	_, err := c.Commit(ctx, repository, "master", "First commit to master", "tester", nil)
	testutil.MustDo(t, "First commit to master", err)

	// create branch based on master
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")

	// add new file
	const newFilename = "/file5"
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", newFilename, nil, "")

	// delete committed file
	const delFilename = "/file1"
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "branch1", delFilename))

	// change/override committed file
	const overFilename = "/file2"
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", overFilename, nil, "seed1")

	// commit changes on son and merge
	_, err = c.Commit(ctx, repository, "branch1", "First commit to branch1", "tester", nil)
	testutil.MustDo(t, "First commit to branch1", err)

	// merge empty branch into master
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", nil)
	if err != nil {
		t.Fatalf("Merge from branch1 to master err=%s, expected none", err)
	}
	if res.CommitID <= 0 {
		t.Fatalf("Merge commit ID = %d, expected valid ID", res.CommitID)
	}

	testVerifyEntries(t, ctx, c, repository, "master", CommittedID, []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
	})

	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
		Difference{Type: DifferenceTypeChanged, Path: "/file2"},
		Difference{Type: DifferenceTypeAdded, Path: "/file5"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
}

func TestCataloger_Merge_FromSonThreeBranches(t *testing.T) {
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
	res, err := c.Merge(ctx, repository, "branch2", "branch1", "tester", nil)
	testutil.MustDo(t, "Merge changes from branch2 to branch1", err)

	if res.CommitID <= 0 {
		t.Errorf("Merge commit ID = %d, expected a valid commit number", res.CommitID)
	}
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeChanged, Path: "/file2"},
		Difference{Type: DifferenceTypeAdded, Path: "/file555"},
		Difference{Type: DifferenceTypeAdded, Path: "/file6"},
		Difference{Type: DifferenceTypeAdded, Path: "/file7"},
		Difference{Type: DifferenceTypeAdded, Path: "/file8"},
		Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}

	testVerifyEntries(t, ctx, c, repository, "branch1", CommittedID, []testEntryInfo{
		{Path: "/file1", Deleted: true},
		{Path: "/file2", Seed: "seed1"},
		{Path: "/file555"},
		{Path: "/file6"},
		{Path: "/file7"},
		{Path: "/file8"},
	})

	// merge the changes from branch1 to master
	res, err = c.Merge(ctx, repository, "branch1", "master", "tester", nil)
	testutil.MustDo(t, "Merge changes from branch1 to master", err)

	// verify valid commit id
	if res.CommitID <= 0 {
		t.Errorf("Merge commit ID = %d, expected a valid commit number", res.CommitID)
	}
	expectedDifferences = Differences{
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

	testVerifyEntries(t, ctx, c, repository, "master", CommittedID, []testEntryInfo{
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

func TestCataloger_Merge_FromSonNewDelSameEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")

	// create new file and commit to branch
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "/file0", nil, "")
	_, err := c.Commit(ctx, repository, "branch1", "Add new file", "tester", nil)
	testutil.MustDo(t, "add new file to branch", err)

	// merge branch to master
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", nil)
	if err != nil {
		t.Fatalf("Merge from branch1 to master err=%s, expected none", err)
	}
	if res.CommitID <= 0 {
		t.Fatalf("Merge commit ID = %d, expected valid ID", res.CommitID)
	}
	testVerifyEntries(t, ctx, c, repository, "master", CommittedID, []testEntryInfo{{Path: "/file0"}})
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeAdded, Path: "/file0"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}

	// delete file on branch and commit
	testutil.MustDo(t, "Delete file0 from branch",
		c.DeleteEntry(ctx, repository, "branch1", "/file0"))
	_, err = c.Commit(ctx, repository, "branch1", "Delete the file", "tester", nil)
	testutil.MustDo(t, "Commit with deleted file", err)

	// merge branch to master
	res, err = c.Merge(ctx, repository, "branch1", "master", "tester", nil)
	if err != nil {
		t.Fatalf("Merge from branch1 to master err=%s, expected none", err)
	}
	if res.CommitID <= 0 {
		t.Fatalf("Merge commit ID = %d, expected valid ID", res.CommitID)
	}
	testVerifyEntries(t, ctx, c, repository, "master", CommittedID, []testEntryInfo{{Path: "/file0", Deleted: true}})
	expectedDifferences = Differences{
		Difference{Type: DifferenceTypeRemoved, Path: "/file0"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
}

func TestCataloger_Merge_FromSonDelModifyGrandfatherFiles(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// create new file and commit to branch
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file0", nil, "")
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file1", nil, "")
	_, err := c.Commit(ctx, repository, "master", "Add new files", "tester", nil)
	testutil.MustDo(t, "add new files to master", err)

	// create branches
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")
	testCatalogerBranch(t, ctx, c, repository, "branch2", "branch1")

	// delete and change files committed on master in branch2
	testutil.MustDo(t, "Delete /file0 from master on branch2",
		c.DeleteEntry(ctx, repository, "branch2", "/file0"))
	testCatalogerCreateEntry(t, ctx, c, repository, "branch2", "/file1", nil, "seed1")
	_, err = c.Commit(ctx, repository, "branch2", "Delete the file", "tester", nil)
	testutil.MustDo(t, "Commit with deleted file", err)

	// merge changes from branch2 to branch1
	res, err := c.Merge(ctx, repository, "branch2", "branch1", "tester", nil)
	if err != nil {
		t.Fatalf("Merge from branch2 to branch1 err=%s, expected none", err)
	}
	if res.CommitID <= 0 {
		t.Fatalf("Merge commit ID = %d, expected valid ID", res.CommitID)
	}
	// verify that the file is deleted (tombstone)
	testVerifyEntries(t, ctx, c, repository, "branch1", CommittedID, []testEntryInfo{
		{Path: "/file0", Deleted: true},
		{Path: "/file1", Seed: "seed1"},
	})
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeRemoved, Path: "/file0"},
		Difference{Type: DifferenceTypeChanged, Path: "/file1"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
}

func TestCataloger_Merge_FromSonConflicts(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// create new file and commit to branch
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file0", nil, "seed0")
	_, err := c.Commit(ctx, repository, "master", "Add new files", "tester", nil)
	testutil.MustDo(t, "add new files to master", err)

	// branch and modify the file
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "/file0", nil, "seed1")
	_, err = c.Commit(ctx, repository, "branch1", "Modify the file", "tester", nil)
	testutil.MustDo(t, "modify /file0 on branch1", err)

	// modify the file on master
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file0", nil, "seed3")
	_, err = c.Commit(ctx, repository, "master", "Modify the file (master)", "tester", nil)
	testutil.MustDo(t, "modify /file0 on master", err)

	// merge changes from branch to master should find the conflict
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", nil)
	if !errors.Is(err, ErrConflictFound) {
		t.Fatalf("Merge from branch1 to master err=%s, expected conflict", err)
	}
	if res.CommitID != 0 {
		t.Fatalf("Merge commit ID = %d, expected 0", res.CommitID)
	}
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeConflict, Path: "/file0"},
	}
	if !res.Differences.Equal(expectedDifferences) {
		t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
}
