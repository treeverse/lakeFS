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
		if res.CommitID != 0 {
			t.Errorf("Failed merge should return no commit id, got %d", res.CommitID)
		}
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
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file5", nil, "")

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
	//_, err = c.Commit(ctx, repository, "branch1", "first commit to branch1", "tester", nil)
	//testutil.MustDo(t, "first commit on branch1", err)

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
	if !reflect.DeepEqual(res.Differences, expectedDifferences) {
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
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file5", nil, "")

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
	if !reflect.DeepEqual(res.Differences, expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}

	// verify that what we got
	_, err = c.GetEntry(ctx, repository, "branch1", CommittedID, "/file1")
	if !errors.As(err, &db.ErrNotFound) {
		t.Fatalf("Expected file1 not to be found, err = %s", err)
	}
	ent2, err := c.GetEntry(ctx, repository, "branch1", CommittedID, "/file2")
	testutil.MustDo(t, "Expected to find an entry /file2", err)
	const ent2Addr = "63564f8303a4c07a145057f7401b64d58c22485f07fcfbe1270539142b1709d3"
	if ent2.PhysicalAddress != ent2Addr {
		t.Errorf("Entry /file2 address %s, expected %s", ent2.PhysicalAddress, ent2Addr)
	}
	const ent5Addr = "53c9486452c01e26833296dcf1f701379fa22f01e610dd9817d064093daab07d"
	testutil.MustDo(t, "Expected to find an entry /file5", err)
	ent5, err := c.GetEntry(ctx, repository, "branch1", CommittedID, "/file5")
	if ent5.PhysicalAddress != ent5Addr {
		t.Errorf("Entry /file5 address %s, expected %s", ent5.PhysicalAddress, ent5Addr)
	}
	testutil.MustDo(t, "Expected to find an entry", err)
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
	if !reflect.DeepEqual(res.Differences, expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(res.Differences), spew.Sdump(expectedDifferences))
	}
	// verify that what we got
	_, err = c.GetEntry(ctx, repository, "branch1", CommittedID, "/file1")
	if !errors.As(err, &db.ErrNotFound) {
		t.Fatalf("Expected file1 not to be found, err = %s", err)
	}
	ent2, err := c.GetEntry(ctx, repository, "branch1", CommittedID, "/file2")
	testutil.MustDo(t, "Expected to find an entry /file2", err)
	const ent2Addr = "63564f8303a4c07a145057f7401b64d58c22485f07fcfbe1270539142b1709d3"
	if ent2.PhysicalAddress != ent2Addr {
		t.Errorf("Entry /file2 address %s, expected %s", ent2.PhysicalAddress, ent2Addr)
	}
	const ent555Addr = "a4708cfbf3a5a84e0cc774da7b273bbb339b56014183e78170075c6330896b85"
	testutil.MustDo(t, "Expected to find an entry /file5", err)
	ent555, err := c.GetEntry(ctx, repository, "branch1", CommittedID, "/file555")
	if ent555.PhysicalAddress != ent555Addr {
		t.Errorf("Entry /file5 address %s, expected %s", ent555.PhysicalAddress, ent555Addr)
	}
	testutil.MustDo(t, "Expected to find an entry", err)
}
