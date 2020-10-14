package catalog

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/go-test/deep"

	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_Merge_FromParentNoChangesInChild(t *testing.T) {
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
	_, err = c.GetEntry(ctx, repository, "branch1:HEAD", delFilename, GetEntryParams{})
	if err != nil {
		t.Fatalf("Get entry %s, expected to be found: %s", delFilename, err)
	}

	// merge master to branch1
	res, err := c.Merge(ctx, repository, "master", "branch1", "tester", "", nil)
	if err != nil {
		t.Fatal("Merge from master to branch1 failed:", err)
	}
	if !IsValidReference(res.Reference) {
		t.Fatalf("Merge reference = %s, expected valid reference", res.Reference)
	}

	testVerifyEntries(t, ctx, c, repository, "branch1", []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
	})

	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
		DifferenceTypeChanged: 1,
		DifferenceTypeAdded:   1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeChanged, Path: "/file2"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file5"},
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	//}
	//if !differences.Equal(expectedDifferences) {
	//	t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}
}

func TestCataloger_Merge_FromParentConflicts(t *testing.T) {
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
	res, err := c.Merge(ctx, repository, "master", "branch1", "tester", "", nil)

	// expected to find 2 conflicts on the files we update/created with the same path
	if !errors.Is(err, ErrConflictFound) {
		t.Errorf("Merge err = %s, expected conflict with err = %s", err, ErrConflictFound)
	}
	if res.Reference != "" {
		t.Errorf("Merge reference = %s, expected to be empty", res.Reference)
	}
	expectedDifferences := Differences{
		Difference{Type: DifferenceTypeConflict, Entry: Entry{Path: "/file2"}},
		Difference{Type: DifferenceTypeConflict, Entry: Entry{Path: "/file5"}},
	}
	if res.Summary[DifferenceTypeConflict] != len(expectedDifferences) {
		t.Fatalf("Merge summary conflicts=%d, expected %d", res.Summary[DifferenceTypeConflict], len(expectedDifferences))
	}
	differences, _, err := c.Diff(ctx, repository, "master", "branch1", -1, "")
	testutil.MustDo(t, "diff merge changes", err)
	if !differences.Equal(expectedDifferences) {
		t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	}
}

func TestCataloger_Merge_FromParentNoChangesInParent(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")
	res, err := c.Merge(ctx, repository, "master", "branch1", "tester", "", nil)
	expectedErr := ErrNoDifferenceWasFound
	if !errors.Is(err, expectedErr) {
		t.Errorf("Merge err = %s, expected %s", err, expectedErr)
	}
	if IsValidReference(res.Reference) {
		t.Errorf("Merge reference = %s, expected valid reference", res.Reference)
	}
	// TODO(barak): enable test after diff between commits is supported
	//commitLog, err := c.GetCommit(ctx, repository, reference)
	//testutil.MustDo(t, "get merge commit reference", err)
	//if len(commitLog.Parents) != 2 {
	//	t.Fatal("merge commit log should have two parents")
	//}
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if len(differences) != 0 {
	//	t.Errorf("Merge differences len=%d, expected 0", len(differences))
	//}
}

func TestCataloger_Merge_FromParentChangesInBoth(t *testing.T) {
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
	res, err := c.Merge(ctx, repository, "master", "branch1", "tester", "", nil)
	if err != nil {
		t.Fatal("Merge from master to branch1 failed:", err)
	}
	if !IsValidReference(res.Reference) {
		t.Errorf("Merge reference = %s, expected a reference commit number", res.Reference)
	}
	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
		DifferenceTypeChanged: 1,
		DifferenceTypeAdded:   1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}

	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	//	Difference{Type: DifferenceTypeChanged, Path: "/file2"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file5"},
	//}
	//if !differences.Equal(expectedDifferences) {
	//	t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}

	testVerifyEntries(t, ctx, c, repository, "branch1", []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
		{Path: "/b2/file0", Seed: "seed2"},
		{Path: "/b2/file1", Seed: "seed2"},
		{Path: "/b2/file2", Seed: "seed2"},
	})
}

func TestCataloger_Merge_FromParentThreeBranches(t *testing.T) {
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
	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
		DifferenceTypeChanged: 1,
		DifferenceTypeAdded:   1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}

	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	//	Difference{Type: DifferenceTypeChanged, Path: "/file2"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file555"},
	//}
	//if !differences.Equal(expectedDifferences) {
	//	t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}

	testVerifyEntries(t, ctx, c, repository, "branch2", []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
	})
}

func TestCataloger_Merge_FromChildNoChanges(t *testing.T) {
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
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", "", nil)
	expectedErr := ErrNoDifferenceWasFound
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Merge from branch1 to master err=%s, expected=%s", err, expectedErr)
	}
	if res == nil {
		t.Fatal("Merge result is nil, expected to have a diff result")
	} else if res.Reference != "" {
		t.Fatalf("Merge reference = %s, expected none", res.Reference)
	}
}

func TestCataloger_Merge_FromChildChangesOnChild(t *testing.T) {
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

	// commit changes on child and merge
	_, err = c.Commit(ctx, repository, "branch1", "First commit to branch1", "tester", nil)
	testutil.MustDo(t, "First commit to branch1", err)

	// merge empty branch into master
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", "", nil)
	if err != nil {
		t.Fatalf("Merge from branch1 to master err=%s, expected none", err)
	}
	if !IsValidReference(res.Reference) {
		t.Fatalf("Merge reference = %s, expected valid reference", res.Reference)
	}
	testVerifyEntries(t, ctx, c, repository, "master", []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
	})

	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
		DifferenceTypeChanged: 1,
		DifferenceTypeAdded:   1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	//	Difference{Type: DifferenceTypeChanged, Path: "/file2"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file5"},
	//}
	//if !differences.Equal(expectedDifferences) {
	//	t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}
}

func TestCataloger_Merge_FromChildThreeBranches(t *testing.T) {
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
	res, err := c.Merge(ctx, repository, "branch2", "branch1", "tester", "", nil)
	testutil.MustDo(t, "Merge changes from branch2 to branch1", err)

	if !IsValidReference(res.Reference) {
		t.Errorf("Merge reference = %s, expected a valid reference", res.Reference)
	}
	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
		DifferenceTypeChanged: 1,
		DifferenceTypeAdded:   4,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeChanged, Path: "/file2"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file555"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file6"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file7"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file8"},
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	//}
	//if !differences.Equal(expectedDifferences) {
	//	t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}

	testVerifyEntries(t, ctx, c, repository, "branch1:HEAD", []testEntryInfo{
		{Path: "/file1", Deleted: true},
		{Path: "/file2", Seed: "seed1"},
		{Path: "/file555"},
		{Path: "/file6"},
		{Path: "/file7"},
		{Path: "/file8"},
	})

	// merge the changes from branch1 to master
	res, err = c.Merge(ctx, repository, "branch1", "master", "tester", "", nil)
	testutil.MustDo(t, "Merge changes from branch1 to master", err)

	// verify valid commit id
	if !IsValidReference(res.Reference) {
		t.Errorf("Merge reference = %s, expected valid reference", res.Reference)
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
		DifferenceTypeChanged: 1,
		DifferenceTypeAdded:   7,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err := c.Diff(ctx, repository, "master", "tester", -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeChanged, Path: "/file2"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file3"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file4"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file5"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file555"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file6"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file7"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file8"},
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	//}
	//if !differences.Equal(expectedDifferences) {
	//	t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}

	testVerifyEntries(t, ctx, c, repository, "master", []testEntryInfo{
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

func TestCataloger_Merge_FromChildNewDelSameEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")

	// create new file and commit to branch
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "/file0", nil, "")
	_, err := c.Commit(ctx, repository, "branch1", "Add new file", "tester", nil)
	testutil.MustDo(t, "add new file to branch", err)

	// merge branch to master
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", "", nil)
	if err != nil {
		t.Fatalf("Merge from branch1 to master err=%s, expected none", err)
	}
	if !IsValidReference(res.Reference) {
		t.Fatalf("Merge reference = %s, expected valid reference", res.Reference)
	}
	testVerifyEntries(t, ctx, c, repository, "master", []testEntryInfo{{Path: "/file0"}})
	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeAdded: 1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeAdded, Path: "/file0"},
	//}
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}

	// delete file on branch and commit
	testutil.MustDo(t, "Delete file0 from branch",
		c.DeleteEntry(ctx, repository, "branch1", "/file0"))
	_, err = c.Commit(ctx, repository, "branch1", "Delete the file", "tester", nil)
	testutil.MustDo(t, "Commit with deleted file", err)

	// merge branch to master
	res, err = c.Merge(ctx, repository, "branch1", "master", "tester", "", nil)
	if err != nil {
		t.Fatalf("Merge from branch1 to master err=%s, expected none", err)
	}
	if !IsValidReference(res.Reference) {
		t.Fatalf("Merge reference = %s, expected valid reference", res.Reference)
	}
	testVerifyEntries(t, ctx, c, repository, "master", []testEntryInfo{{Path: "/file0", Deleted: true}})
	commitLog, err = c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences = Differences{
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file0"},
	//}
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}
	//if !differences.Equal(expectedDifferences) {
	//	t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}
}

func TestCataloger_Merge_FromChildNewEntrySameEntry(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")

	// create new file and commit to branch
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "/file0", nil, "")
	_, err := c.Commit(ctx, repository, "branch1", "Add new file", "tester", nil)
	testutil.MustDo(t, "add new file to branch", err)

	// merge branch to master
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", "", nil)
	if err != nil {
		t.Fatalf("Merge from branch1 to master err=%s, expected none", err)
	}
	if !IsValidReference(res.Reference) {
		t.Fatalf("Merge reference = %s, expected valid reference", res.Reference)
	}
	testVerifyEntries(t, ctx, c, repository, "master", []testEntryInfo{{Path: "/file0"}})

	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeAdded: 1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeAdded, Path: "/file0"},
	//}
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}

	// create same file and commit to branch
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "/file0", nil, "")
	_, err = c.Commit(ctx, repository, "branch1", "Add same file", "tester", nil)
	testutil.MustDo(t, "add same file to branch", err)

	// merge branch to master
	res, err = c.Merge(ctx, repository, "branch1", "master", "tester", "", nil)
	if err != nil {
		t.Fatalf("Merge from branch1 to master err=%s, expected none", err)
	}
	if !IsValidReference(res.Reference) {
		t.Fatalf("Merge reference = %s, expected valid reference", res.Reference)
	}
	commitLog, err = c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences = Differences{}
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}
}

func TestCataloger_Merge_FromChildDelModifyGrandparentFiles(t *testing.T) {
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
	res, err := c.Merge(ctx, repository, "branch2", "branch1", "tester", "", nil)
	if err != nil {
		t.Fatalf("Merge from branch2 to branch1 err=%s, expected none", err)
	}
	if !IsValidReference(res.Reference) {
		t.Fatalf("Merge reference = %s, expected valid reference", res.Reference)
	}
	// verify that the file is deleted (tombstone)
	testVerifyEntries(t, ctx, c, repository, "branch1", []testEntryInfo{
		{Path: "/file0", Deleted: true},
		{Path: "/file1", Seed: "seed1"},
	})
	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
		DifferenceTypeChanged: 1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file0"},
	//	Difference{Type: DifferenceTypeChanged, Path: "/file1"},
	//}
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}
}

func TestCataloger_Merge_FromChildConflicts(t *testing.T) {
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
	res, err := c.Merge(ctx, repository, "branch1", "master", "tester", "", nil)
	if !errors.Is(err, ErrConflictFound) {
		t.Fatalf("Merge from branch1 to master err=%s, expected conflict", err)
	}
	if res == nil {
		t.Fatal("Merge result is nil, expected to have value in case of conflict")
	} else if res.Reference != "" {
		t.Fatalf("Merge reference = %s, expected none", res.Reference)
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeConflict: 1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeConflict, Path: "/file0"},
	//}
	//commitLog, err := c.GetCommit(ctx, repository, reference)
	//testutil.MustDo(t, "get merge commit reference", err)
	//if len(commitLog.Parents) != 2 {
	//	t.Fatal("merge commit log should have two parents")
	//}
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Fatalf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}
}

func TestCataloger_Merge_FromParentThreeBranchesExtended1(t *testing.T) {
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
	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
		DifferenceTypeChanged: 1,
		DifferenceTypeAdded:   1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences := Differences{
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file1"},
	//	Difference{Type: DifferenceTypeChanged, Path: "/file2"},
	//	Difference{Type: DifferenceTypeAdded, Path: "/file555"},
	//}
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}

	testVerifyEntries(t, ctx, c, repository, "branch2", []testEntryInfo{
		{Path: newFilename},
		{Path: overFilename, Seed: "seed1"},
		{Path: delFilename, Deleted: true},
	})
	// test that an object deleted at master becomes deleted at branch2 only after merges from both parents
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", "/file0"))
	_, err = c.Commit(ctx, repository, "master", "commit file0 deletion", "tester", nil)
	testutil.Must(t, err)

	testCatalogerGetEntry(t, ctx, c, repository, "branch2", "/file0", true)
	testCatalogerGetEntry(t, ctx, c, repository, "branch1", "/file0", true)
	testCatalogerGetEntry(t, ctx, c, repository, "master", "/file0", false)

	_, err = c.Merge(ctx, repository, "master", "branch1", "tester", "", nil)
	testutil.MustDo(t, "merge to master to branch1", err)

	testCatalogerGetEntry(t, ctx, c, repository, "branch2", "/file0", true)
	testCatalogerGetEntry(t, ctx, c, repository, "branch1", "/file0", false)
	testCatalogerGetEntry(t, ctx, c, repository, "master", "/file0", false)

	_, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "", nil)
	testutil.MustDo(t, "merge branch1 to branch2", err)

	testCatalogerGetEntry(t, ctx, c, repository, "branch2", "/file0", false)
	testCatalogerGetEntry(t, ctx, c, repository, "branch1", "/file0", false)
	testCatalogerGetEntry(t, ctx, c, repository, "master", "/file0", false)

	// test that the same object with the same name does not create a conflict in child to parent , and is not a change

	testCatalogerCreateEntry(t, ctx, c, repository, "branch2", "/file0", nil, "seed1")
	_, _ = c.Commit(ctx, repository, "branch2", "commit file0 creation", "tester", nil)
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file0", nil, "seed1")
	_, _ = c.Commit(ctx, repository, "master", "commit file0 creation", "tester", nil)
	res, err = c.Merge(ctx, repository, "master", "branch1", "tester", "", nil)
	testutil.MustDo(t, "merge master to branch1", err)
	if res.Reference == "" {
		t.Fatal("No merge reference")
	}
	commitLog, err = c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeAdded: 1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences = Differences{
	//	Difference{Type: DifferenceTypeAdded, Path: "/file0"},
	//}
	//differences, _, err = c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}
	res, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "", nil)
	testutil.MustDo(t, "merge branch1 to branch2", err)
	if res.Reference == "" {
		t.Fatal("No merge results")
	}
	commitLog, err = c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err = c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if len(differences) != 0 {
	//	t.Errorf("unexpected Merge differences = %s", spew.Sdump(differences))
	//}

	// deletion in master will force  physically delete in grandchild
	testutil.MustDo(t, "delete committed file on master",
		c.DeleteEntry(ctx, repository, "master", "/file0"))
	_, err = c.Commit(ctx, repository, "master", "commit file0 deletion", "tester", nil)
	testutil.MustDo(t, "commit file0 delete", err)
	res, err = c.Merge(ctx, repository, "master", "branch1", "tester", "bubling /file0 deletion up", nil)
	testutil.MustDo(t, "merge master to branch1", err)
	if res.Reference == "" {
		t.Fatal("No merge reference")
	}

	res, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "forcing file0 on branch2 to delete", nil)
	testutil.MustDo(t, "merge master to branch1", err)
	if res.Reference == "" {
		t.Fatal("No merge reference")
	}

	commitLog, err = c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences = Differences{
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file0"},
	//}
	//differences, _, err = c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}

	//identical entries created in child and grandparent do not create conflict - even when grandparent is uncommitted
	_, err = c.Merge(ctx, repository, "branch2", "branch1", "tester", "empty updates", nil)
	testutil.MustDo(t, "merge branch2 to branch1", err)

	_, err = c.Merge(ctx, repository, "branch1", "master", "tester", "empty updates", nil)
	testutil.MustDo(t, "merge branch1 to master", err)

	testCatalogerCreateEntry(t, ctx, c, repository, "branch2", "/file111", nil, "seed1")
	_, err = c.Commit(ctx, repository, "branch2", "commit file0 creation", "tester", nil)
	testutil.MustDo(t, "commit file0 creation to branch2", err)

	testCatalogerCreateEntry(t, ctx, c, repository, "master", "/file111", nil, "seed1")
	_, err = c.Merge(ctx, repository, "branch2", "branch1", "tester", "pushing /file111 down", nil)
	testutil.MustDo(t, "merge branch2 to branch1", err)

	_, err = c.Merge(ctx, repository, "branch1", "master", "tester", "pushing /file111 down", nil)
	testutil.MustDo(t, "merge branch1 to master", err)

	// push file111 delete
	_, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "delete /file111 up", nil)
	testutil.Must(t, err)

	testutil.MustDo(t, "delete committed file on branch1",
		c.DeleteEntry(ctx, repository, "branch1", "/file111"))
	_, err = c.Commit(ctx, repository, "branch1", "commit file111 deletion", "tester", nil)
	testutil.MustDo(t, "commit file111 to branch1", err)

	res, err = c.Merge(ctx, repository, "branch1", "branch2", "tester", "delete /file111 up", nil)
	testutil.MustDo(t, "merge branch1 to branch2", err)
	if res.Reference == "" {
		t.Fatal("No merge results")
	}
	commitLog, err = c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeRemoved: 1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences = Differences{
	//	Difference{Type: DifferenceTypeRemoved, Path: "/file111"},
	//}
	//differences, _, err = c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}

	res, err = c.Merge(ctx, repository, "branch1", "master", "tester", "try delete /file111 . get conflict", nil)
	if !errors.Is(err, ErrConflictFound) {
		t.Fatalf("Expected to get conflict error, got err=%+v", err)
	}
	if res == nil {
		t.Fatal("Expected merge result, got none")
	} else if res.Reference != "" {
		t.Fatalf("Expected empty reference, got %s", res.Reference)
	}
	if diff := deep.Equal(res.Summary, map[DifferenceType]int{
		DifferenceTypeConflict: 1,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//expectedDifferences = Differences{
	//	Difference{Type: DifferenceTypeConflict, Path: "/file111"},
	//}
	//differences, _, err = c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//if !differences.Equal(expectedDifferences) {
	//	t.Errorf("Merge differences = %s, expected %s", spew.Sdump(differences), spew.Sdump(expectedDifferences))
	//}
}

func TestCataloger_MergeOverDeletedEntries(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	// setup a report with 'master' with a single file, and branch 'b1' that started after the file was committed
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "fileX", nil, "master")
	_, err := c.Commit(ctx, repository, "master", "fileX", "tester", nil)
	testutil.MustDo(t, "commit file first time on master", err)
	_, err = c.CreateBranch(ctx, repository, "b1", "master")
	testutil.MustDo(t, "create branch b1", err)

	// delete file on 'b1', commit and check that we don't get the file on 'b1' branch
	err = c.DeleteEntry(ctx, repository, "b1", "fileX")
	testutil.MustDo(t, "delete file on branch b1", err)
	_, err = c.Commit(ctx, repository, "b1", "fileX", "tester", nil)
	testutil.MustDo(t, "commit file delete on b1", err)
	_, err = c.GetEntry(ctx, repository, "b1", "fileX", GetEntryParams{})
	if !errors.Is(err, ErrEntryNotFound) {
		t.Fatal("expected entry not found, got", err)
	}
	_, err = c.Merge(ctx, repository, "master", "b1", "tester", "merge changes from master to b1 part 2", nil)
	testutil.MustDo(t, "merge master to b1 part 2", err)

	// create and commit the same file, different content, on 'master', merge to 'b1' and check that we get the file on 'b1'
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "fileX", nil, "master2")
	_, err = c.Commit(ctx, repository, "master", "fileX", "tester", nil)
	_, err = c.Merge(ctx, repository, "master", "b1", "tester", "merge changes from master to b1", nil)
	testutil.MustDo(t, "merge master to b1", err)
	ent, err := c.GetEntry(ctx, repository, "b1", "fileX", GetEntryParams{})
	testutil.MustDo(t, "get entry again from b1", err)
	expectedChecksum := testCreateEntryCalcChecksum("fileX", "master2")
	if ent.Checksum != expectedChecksum {
		t.Fatalf("Get file checksum after merge=%s, expected %s", ent.Checksum, expectedChecksum)
	}
}

func TestCataloger_MergeWitoutDiff(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	// setup a report with 'master' with a single file, and branch 'b1' that started after the file was committed
	repository := testCatalogerRepo(t, ctx, c, "repository", "master")
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "fileX", nil, "master")
	_, err := c.Commit(ctx, repository, "master", "fileX", "tester", nil)
	testutil.MustDo(t, "commit file first time on master", err)
	_, err = c.CreateBranch(ctx, repository, "b1", "master")
	testutil.MustDo(t, "create branch b1", err)
	_, err = c.Merge(ctx, repository, "master", "b1", "tester", "merge nothing from master to b1", nil)
	if err.Error() != "no difference was found" {
		t.Fatal("did not get 'nothing to commit' error")
	}
	testCatalogerCreateEntry(t, ctx, c, repository, "master", "file_dummy", nil, "master1")
	_, err = c.Commit(ctx, repository, "master", "file_dummy", "tester", nil)
	testutil.MustDo(t, "commit dummy file  master", err)
	err = c.DeleteEntry(ctx, repository, "master", "file_dummy")
	testutil.MustDo(t, "delete dummy_file on master", err)
	_, err = c.Commit(ctx, repository, "master", "file_dummy delete", "tester", nil)
	testutil.MustDo(t, "commit dummy file  deletion", err)
	_, err = c.Merge(ctx, repository, "master", "b1", "tester", "merge nothing from master to b1", nil)
	if err != nil {
		t.Fatalf("error on merge with no changes:%+v", err)
	}
	_, err = c.Merge(ctx, repository, "master", "b1", "tester", "merge nothing from master to b1", nil)
	if err.Error() != "no difference was found" {
		t.Fatal("did not get 'nothing to commit' error")
	}
}
