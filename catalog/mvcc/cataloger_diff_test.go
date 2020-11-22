package mvcc

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_DiffEmpty(t *testing.T) {
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

	res, hasMore, err := c.Diff(ctx, repository, "master", "master", DiffParams{Limit: 10})
	testutil.MustDo(t, "Diff", err)
	if len(res) != 0 {
		t.Errorf("Diff: got %+v but expected nothing", res)
	}
	if hasMore {
		t.Errorf("Diff: got *more* diffs but expected nothing")
	}
}

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
	var differences catalog.Differences
	for {
		res, hasMore, err := c.Diff(ctx, repository, "branch1", "master", catalog.DiffParams{
			Limit: limit,
			After: after,
		})
		testutil.MustDo(t, "list diff changes", err)
		if len(res) > limit {
			t.Fatalf("Diff() result length=%d, expected no more than %d", len(res), limit)
		}
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
		var d *catalog.Difference
		for diffIdx := range differences {
			if differences[diffIdx].Path == name {
				d = &differences[diffIdx]
			}
		}
		// verify diff record
		if d == nil {
			t.Fatalf("Missing diff for path=%s", name)
		}
		var expectedType catalog.DifferenceType
		switch {
		case i < 5:
			expectedType = catalog.DifferenceTypeRemoved
		case i >= 10:
			expectedType = catalog.DifferenceTypeAdded
		default:
			expectedType = catalog.DifferenceTypeChanged
		}
		if d.Type != expectedType {
			t.Fatalf("Path '%s' diff type=%d, expected=%d", d.Path, d.Type, expectedType)
		}
	}

	// check the case of 0 amount
	res, hasMore, err := c.Diff(ctx, repository, "branch1", "master", catalog.DiffParams{Limit: 0})
	testutil.MustDo(t, "list diff changes with 0 limit", err)
	if !hasMore {
		t.Error("Diff() limit 0 hasMore should be true")
	}
	if len(res) != 0 {
		t.Errorf("Diff() limit 0 len results is %d, expected none", len(res))
	}
}

func TestCataloger_Diff_FromChild(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", catalog.DefaultBranchName)

	// create 3 files and commit
	const numberOfEntries = 3
	for i := 0; i < numberOfEntries; i++ {
		p := fmt.Sprintf("file%d", i)
		testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, p, nil, catalog.DefaultBranchName)
	}
	_, err := c.Commit(ctx, repository, catalog.DefaultBranchName, "initial commit", "tester", nil)
	testutil.MustDo(t, "initial commit", err)

	// branch changes into child branch called "branch1"
	testCatalogerBranch(t, ctx, c, repository, "branch1", catalog.DefaultBranchName)

	// branch1 - delete
	err = c.DeleteEntry(ctx, repository, "branch1", "file1")
	testutil.MustDo(t, "delete entry from branch", err)

	// branch1 - update
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "file2", nil, "branch1")

	// branch1 - add
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "fileX", nil, "branch1")

	// commit change on "branch1"
	_, err = c.Commit(ctx, repository, "branch1", "commit changes", "tester", nil)
	testutil.MustDo(t, "commit changes", err)

	// diff changes between "branch1" and "master" (from child)
	res, more, err := c.Diff(ctx, repository, "branch1", catalog.DefaultBranchName, catalog.DiffParams{Limit: -1})
	testutil.MustDo(t, "Diff changes between branch1 and master", err)
	if more {
		t.Fatal("Diff has more than expected differences")
	}
	clearChecksum(&res)
	if diff := deep.Equal(res, catalog.Differences{
		catalog.Difference{Entry: catalog.Entry{Path: "file1"}, Type: catalog.DifferenceTypeRemoved},
		catalog.Difference{Entry: catalog.Entry{Path: "file2"}, Type: catalog.DifferenceTypeChanged},
		catalog.Difference{Entry: catalog.Entry{Path: "fileX"}, Type: catalog.DifferenceTypeAdded},
	}); diff != nil {
		t.Fatal("Diff unexpected differences:", diff)
	}
}

func TestCataloger_Diff_SameBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	// create 3 files and commit
	const numberOfEntries = 3
	for j := 0; j < numberOfEntries; j++ {
		p := fmt.Sprintf("file%d-%s", j, catalog.DefaultBranchName)
		testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, p, nil, catalog.DefaultBranchName)
	}
	firstCommit, err := c.Commit(ctx, repository, catalog.DefaultBranchName, "commit changes to "+catalog.DefaultBranchName, "tester", nil)
	testutil.MustDo(t, "initial branch commit", err)

	// delete
	err = c.DeleteEntry(ctx, repository, catalog.DefaultBranchName, "file1-"+catalog.DefaultBranchName)
	testutil.MustDo(t, "delete entry from branch", err)

	// update
	testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, "file2-"+catalog.DefaultBranchName, nil, catalog.DefaultBranchName+"mod")

	// add
	testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, "fileX-"+catalog.DefaultBranchName, nil, catalog.DefaultBranchName)

	// commit changes
	secondCommit, err := c.Commit(ctx, repository, catalog.DefaultBranchName, "commit changes", "tester", nil)
	testutil.MustDo(t, "commit branch changes", err)

	// diff changes between second and first commit
	res, more, err := c.Diff(ctx, repository, secondCommit.Reference, firstCommit.Reference, catalog.DiffParams{Limit: -1})
	testutil.MustDo(t, "Diff changes from second and first commits", err)
	if more {
		t.Fatal("Diff has more than expected differences")
	}
	clearChecksum(&res)
	if diff := deep.Equal(res, catalog.Differences{
		catalog.Difference{Entry: catalog.Entry{Path: "file1-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeRemoved},
		catalog.Difference{Entry: catalog.Entry{Path: "file2-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeChanged},
		catalog.Difference{Entry: catalog.Entry{Path: "fileX-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeAdded},
	}); diff != nil {
		t.Fatal("Diff unexpected differences:", diff)
	}

	// diff changes between first and second commit
	res, more, err = c.Diff(ctx, repository, firstCommit.Reference, secondCommit.Reference, catalog.DiffParams{Limit: -1})
	testutil.MustDo(t, "Diff changes from first and second commits", err)
	if more {
		t.Fatal("Diff has more than expected differences")
	}
	clearChecksum(&res)
	if diff := deep.Equal(res, catalog.Differences{
		catalog.Difference{Entry: catalog.Entry{Path: "file1-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeAdded},
		catalog.Difference{Entry: catalog.Entry{Path: "file2-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeChanged},
	}); diff != nil {
		t.Fatal("Diff unexpected differences:", diff)
	}
}

// TestCataloger_Diff_SameBranchDiffMergedChanges test changes we merge from parent are found in diff between two commits
func TestCataloger_Diff_SameBranchDiffMergedChanges(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", catalog.DefaultBranchName)

	testCatalogerBranch(t, ctx, c, repository, "branch1", catalog.DefaultBranchName)
	const numberOfEntries = 3
	for j := 0; j < numberOfEntries; j++ {
		p := fmt.Sprintf("file%d-%s", j, catalog.DefaultBranchName)
		testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, p, nil, catalog.DefaultBranchName)
	}
	// commit and merge changes
	_, err := c.Commit(ctx, repository, catalog.DefaultBranchName, "commit changes to "+catalog.DefaultBranchName, "tester", nil)
	testutil.MustDo(t, "initial branch commit", err)
	firstCommit, err := c.Merge(ctx, repository, catalog.DefaultBranchName, "branch1", "tester", "merge changes from master to branch1", nil)
	testutil.MustDo(t, "merge changes from master to branch1", err)

	// delete
	err = c.DeleteEntry(ctx, repository, catalog.DefaultBranchName, "file1-"+catalog.DefaultBranchName)
	testutil.MustDo(t, "delete entry from branch", err)

	// update
	testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, "file2-"+catalog.DefaultBranchName, nil, catalog.DefaultBranchName+"mod")

	// add
	testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, "fileX-"+catalog.DefaultBranchName, nil, catalog.DefaultBranchName)

	// commit and merge changes
	_, err = c.Commit(ctx, repository, catalog.DefaultBranchName, "commit changes", "tester", nil)
	testutil.MustDo(t, "commit branch changes", err)
	secondCommit, err := c.Merge(ctx, repository, catalog.DefaultBranchName, "branch1", "tester", "merge more changes from master to branch1", nil)
	testutil.MustDo(t, "merge more changes from master to branch1", err)

	// diff changes between second and first commit
	res, more, err := c.Diff(ctx, repository, secondCommit.Reference, firstCommit.Reference, catalog.DiffParams{Limit: -1})
	testutil.MustDo(t, "Diff changes from second and first commits", err)
	if more {
		t.Fatal("Diff has more than expected differences")
	}
	clearChecksum(&res)
	if diff := deep.Equal(res, catalog.Differences{
		catalog.Difference{Entry: catalog.Entry{Path: "file1-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeRemoved},
		catalog.Difference{Entry: catalog.Entry{Path: "file2-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeChanged},
		catalog.Difference{Entry: catalog.Entry{Path: "fileX-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeAdded},
	}); diff != nil {
		t.Fatal("Diff unexpected differences:", diff)
	}

	// diff changes between first and second commit
	res, more, err = c.Diff(ctx, repository, firstCommit.Reference, secondCommit.Reference, catalog.DiffParams{Limit: -1})
	testutil.MustDo(t, "Diff changes from first and second commits", err)
	if more {
		t.Fatal("Diff has more than expected differences")
	}
	clearChecksum(&res)
	if diff := deep.Equal(res, catalog.Differences{
		catalog.Difference{Entry: catalog.Entry{Path: "file1-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeAdded},
		catalog.Difference{Entry: catalog.Entry{Path: "file2-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeChanged},
	}); diff != nil {
		t.Fatal("Diff unexpected differences:", diff)
	}

	// rewrite a file with different content and expect to find a change in diff
	testCatalogerCreateEntry(t, ctx, c, repository, "branch1", "file2-"+catalog.DefaultBranchName, nil, catalog.DefaultBranchName+"mod2")
	rewriteCommit, err := c.Commit(ctx, repository, "branch1", "rewrite file2", "tester", nil)
	testutil.MustDo(t, "rewrite file2", err)

	res, more, err = c.Diff(ctx, repository, rewriteCommit.Reference, secondCommit.Reference, catalog.DiffParams{Limit: -1})
	testutil.MustDo(t, "Diff changes from rewrite and second commits", err)
	if more {
		t.Fatal("Diff has more than expected differences")
	}
	clearChecksum(&res)
	if diff := deep.Equal(res, catalog.Differences{
		catalog.Difference{Entry: catalog.Entry{Path: "file2-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeChanged},
	}); diff != nil {
		t.Fatal("Diff unexpected differences:", diff)
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
	if diff := deep.Equal(res.Summary, map[catalog.DifferenceType]int{
		catalog.DifferenceTypeRemoved: 1,
		catalog.DifferenceTypeChanged: 1,
		catalog.DifferenceTypeAdded:   7,
	}); diff != nil {
		t.Fatal("Merge Summary", diff)
	}
	// TODO(barak): enable test after diff between commits is supported
	//differences, _, err := c.Diff(ctx, repository, commitLog.Parents[0], commitLog.Parents[1], -1, "")
	//testutil.MustDo(t, "diff merge changes", err)
	//expectedDifferences := catalog.Differences{
	//	catalog.Difference{Type: catalog.DifferenceTypeChanged, Path: "/file2"},
	//	catalog.Difference{Type: catalog.DifferenceTypeAdded, Path: "/file3"},
	//	catalog.Difference{Type: catalog.DifferenceTypeAdded, Path: "/file4"},
	//	catalog.Difference{Type: catalog.DifferenceTypeAdded, Path: "/file5"},
	//	catalog.Difference{Type: catalog.DifferenceTypeAdded, Path: "/file555"},
	//	catalog.Difference{Type: catalog.DifferenceTypeAdded, Path: "/file6"},
	//	catalog.Difference{Type: catalog.DifferenceTypeAdded, Path: "/file7"},
	//	catalog.Difference{Type: catalog.DifferenceTypeAdded, Path: "/file8"},
	//	catalog.Difference{Type: catalog.DifferenceTypeRemoved, Path: "/file1"},
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

func TestCataloger_Diff_FromParentThreeBranches(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	const numberOfBranches = 3
	const numberOfEntries = 3

	// create 3 files on master
	for j := 0; j < numberOfEntries; j++ {
		p := fmt.Sprintf("file%d-%s", j, catalog.DefaultBranchName)
		testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, p, nil, catalog.DefaultBranchName)
	}
	_, err := c.Commit(ctx, repository, catalog.DefaultBranchName, "commit changes to "+catalog.DefaultBranchName, "tester", nil)
	testutil.MustDo(t, "initial branch commit", err)

	// create 3 branches, create 3 files and commit. each branch branches from previous branch
	prevBranch := catalog.DefaultBranchName
	for i := 0; i < numberOfBranches; i++ {
		branchName := fmt.Sprintf("branch%d", i)
		testCatalogerBranch(t, ctx, c, repository, branchName, prevBranch)
		for j := 0; j < numberOfEntries; j++ {
			p := fmt.Sprintf("file%d-%s", j, branchName)
			testCatalogerCreateEntry(t, ctx, c, repository, branchName, p, nil, branchName)
		}
		_, err := c.Commit(ctx, repository, branchName, "commit changes to "+branchName, "tester", nil)
		testutil.MustDo(t, "initial branch commit", err)
		prevBranch = branchName
	}

	// on master branch do: delete, update, entry and commit

	// delete
	err = c.DeleteEntry(ctx, repository, catalog.DefaultBranchName, "file1-"+catalog.DefaultBranchName)
	testutil.MustDo(t, "delete entry from branch", err)

	// update
	testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, "file2-"+catalog.DefaultBranchName, nil, catalog.DefaultBranchName+"mod")

	// add
	testCatalogerCreateEntry(t, ctx, c, repository, catalog.DefaultBranchName, "fileX-"+catalog.DefaultBranchName, nil, catalog.DefaultBranchName)

	// commit changes
	_, err = c.Commit(ctx, repository, catalog.DefaultBranchName, "commit changes", "tester", nil)
	testutil.MustDo(t, "commit branch changes", err)

	// diff changes between master and branch0
	res, more, err := c.Diff(ctx, repository, "master", "branch0", catalog.DiffParams{Limit: -1})
	testutil.MustDo(t, "Diff changes from master to branch0", err)
	if more {
		t.Fatal("Diff has more than expected differences")
	}
	clearChecksum(&res)
	if diff := deep.Equal(res, catalog.Differences{
		catalog.Difference{Entry: catalog.Entry{Path: "file1-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeRemoved},
		catalog.Difference{Entry: catalog.Entry{Path: "file2-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeChanged},
		catalog.Difference{Entry: catalog.Entry{Path: "fileX-" + catalog.DefaultBranchName}, Type: catalog.DifferenceTypeAdded},
	}); diff != nil {
		t.Fatal("Diff unexpected differences:", diff)
	}
}

func TestCataloger_Diff_AdditionalFields(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// test flow
	// 1. create repo (with master branch)
	// 2. create branch based on master - branch1
	// 3. create 3 entries and commit on master
	// 4. run diff between master and branch1 - no additional fields - check physical address is empty
	// 4. run diff between master and branch1 - physical address as additional field - check physical address is set

	repository := testCatalogerRepo(t, ctx, c, "repo", "master")
	testCatalogerBranch(t, ctx, c, repository, "branch1", "master")
	const numOfEntries = 3
	for i := 0; i < numOfEntries; i++ {
		testCatalogerCreateEntry(t, ctx, c, repository, "master", fmt.Sprintf("file%d", i), nil, "")
	}
	_, err := c.Commit(ctx, repository, "master", "checking changes on master", "tester", nil)
	testutil.Must(t, err)

	res, hasMore, err := c.Diff(ctx, repository, "master", "branch1", catalog.DiffParams{Limit: numOfEntries})
	testutil.MustDo(t, "diff changes", err)
	if hasMore {
		t.Fatal("Diff() hasMore should be false")
	}
	expectedLen := 3
	if len(res) != expectedLen {
		t.Fatalf("Diff() len of result %d, expected %d", len(res), expectedLen)
	}
	for _, d := range res {
		if d.PhysicalAddress != "" {
			t.Fatalf("Diff result entry should not have physical address set (%s)", d.PhysicalAddress)
		}
	}

	res, hasMore, err = c.Diff(ctx, repository, "master", "branch1", catalog.DiffParams{
		Limit:            numOfEntries,
		AdditionalFields: []string{catalog.DBEntryFieldPhysicalAddress, catalog.DBEntryFieldChecksum},
	})
	testutil.MustDo(t, "diff changes", err)
	if hasMore {
		t.Fatal("Diff() hasMore should be false")
	}
	if len(res) != expectedLen {
		t.Fatalf("Diff() len of result %d, expected %d", len(res), expectedLen)
	}
	for _, d := range res {
		if d.PhysicalAddress == "" {
			t.Fatalf("Diff result entry should not have physical address set (%s)", d.PhysicalAddress)
		}
		// verify that checksum - added by diff code is set on entry
		if d.Checksum == "" {
			t.Fatalf("Diff result entry should not have checksum address set (%s)", d.Checksum)
		}
	}
}

func clearChecksum(d *catalog.Differences) {
	for i := range *d {
		(*d)[i].Entry.Checksum = ""
	}
}
