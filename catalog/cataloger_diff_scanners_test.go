package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_Diff_Scanner_FromParentThreeBranches(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)
	//conn, _ := testutil.GetDB(t, databaseURI)
	repository := testCatalogerRepo(t, ctx, c, "repo", "master")

	const numberOfBranches = 3
	const numberOfEntries = 3

	// create 3 files on master
	for j := 0; j < numberOfEntries; j++ {
		p := fmt.Sprintf("file%d-%s", j, DefaultBranchName)
		testCatalogerCreateEntry(t, ctx, c, repository, DefaultBranchName, p, nil, DefaultBranchName)
	}
	_, err := c.Commit(ctx, repository, DefaultBranchName, "commit changes to "+DefaultBranchName, "tester", nil)
	testutil.MustDo(t, "initial branch commit", err)

	// create 3 branches, create 3 files and commit. each branch branches from previous branch
	prevBranch := DefaultBranchName
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
	err = c.DeleteEntry(ctx, repository, DefaultBranchName, "file1-"+DefaultBranchName)
	testutil.MustDo(t, "delete entry from branch", err)

	// update
	testCatalogerCreateEntry(t, ctx, c, repository, DefaultBranchName, "file2-"+DefaultBranchName, nil, DefaultBranchName+"mod")

	// add
	testCatalogerCreateEntry(t, ctx, c, repository, DefaultBranchName, "fileX-"+DefaultBranchName, nil, DefaultBranchName)

	// commit changes
	_, err = c.Commit(ctx, repository, DefaultBranchName, "commit changes", "tester", nil)
	testutil.MustDo(t, "commit branch changes", err)

	//diff changes between master and branch0
	res, more, err := c.Diff(ctx, repository, "master", "branch0", DiffParams{Limit: -1})

	testutil.MustDo(t, "Diff changes from master to branch0", err)
	if more {
		t.Fatal("Diff has more differences, expected none")
	}
	//res, err := conn.Transact(func(tx db.Tx) (interface{}, error) {
	//	diffScan, err := c.newDiffFromParent(tx, DiffParams{Limit: -1})
	//})

	if diff := deep.Equal(res, Differences{
		Difference{Entry: Entry{Path: "file1-" + DefaultBranchName}, Type: DifferenceTypeRemoved},
		Difference{Entry: Entry{Path: "file2-" + DefaultBranchName}, Type: DifferenceTypeChanged},
		Difference{Entry: Entry{Path: "fileX-" + DefaultBranchName}, Type: DifferenceTypeAdded},
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

	res, hasMore, err := c.Diff(ctx, repository, "master", "branch1", DiffParams{Limit: numOfEntries})
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

	res, hasMore, err = c.Diff(ctx, repository, "master", "branch1", DiffParams{
		Limit:            numOfEntries,
		AdditionalFields: []string{DBEntryFieldPhysicalAddress, DBEntryFieldChecksum},
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
			t.Fatalf("Diff result entry should  have physical address set (%s)", d.PhysicalAddress)
		}
		// verify that checksum - added by diff code is set on entry
		if d.Checksum == "" {
			t.Fatalf("Diff result entry should  have checksum address set (%s)", d.Checksum)
		}
	}
}
