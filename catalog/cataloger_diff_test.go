package catalog

import (
	"context"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_Diff(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// create N files and commit
	commitChanges := func(n int, msg, branch string) {
		for i := 0; i < n; i++ {
			testCatalogerCreateEntry(t, ctx, c, repository, branch, "/file"+strconv.Itoa(i), nil, branch)
		}
		_, err := c.Commit(ctx, repository, branch, msg, "tester", nil)
		testutil.MustDo(t, msg, err)
	}
	commitChanges(10, "Changes on master", "master")
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")
	commitChanges(20, "Changes on branch1", "branch1")
	// delete some files and commit
	for i := 0; i < 5; i++ {
		testutil.MustDo(t, "delete file from branch",
			c.DeleteEntry(ctx, repository, "branch1", "/file"+strconv.Itoa(i)))
	}
	_, err := c.Commit(ctx, repository, "branch1", "delete some files", "tester", nil)
	testutil.MustDo(t, "delete some files from branch1", err)

	const limit = 3
	var after string
	var differences Differences
	for {
		res, hasMore, err := c.Diff(ctx, repository, "branch1", "master", limit, after)
		testutil.MustDo(t, "list diff changes", err)
		differences = append(differences, res...)
		if !hasMore {
			break
		}
		after = res[len(res)-1].Path
	}

	const expectedDifferencesLen = 20
	if len(differences) != expectedDifferencesLen {
		t.Fatalf("Differences len=%d, expected=%d", len(differences), expectedDifferencesLen)
	}
	for i := 0; i < expectedDifferencesLen; i++ {
		// lookup item in diff
		name := "/file" + strconv.Itoa(i)
		var d *Difference
		for diffIdx := range differences {
			if differences[diffIdx].Path == name {
				d = &differences[diffIdx]
			}
		}
		// verify diff record
		if d == nil {
			t.Fatalf("Missing diff for path=%s", name)
		}
		var expectedType DifferenceType
		switch {
		case i < 5:
			expectedType = DifferenceTypeRemoved
		case i >= 10:
			expectedType = DifferenceTypeAdded
		default:
			expectedType = DifferenceTypeChanged
		}
		if d.Type != expectedType {
			t.Fatalf("Path '%s' diff type=%d, expected=%d", d.Path, d.Type, expectedType)
		}
	}
}

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
	commitLog, err := c.GetCommit(ctx, repository, res.Reference)
	testutil.MustDo(t, "get merge commit reference", err)
	if len(commitLog.Parents) != 2 {
		t.Fatal("merge commit log should have two parents")
	}
	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
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
