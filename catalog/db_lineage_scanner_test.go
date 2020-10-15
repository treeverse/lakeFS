package catalog

import (
	"context"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func TestDBLineageScanner(t *testing.T) {
	const numberOfObjects = 16

	ctx := context.Background()
	conn, uri := testutil.GetDB(t, databaseURI)
	defer func() { _ = conn.Close() }()

	c := TestCataloger{Cataloger: NewCataloger(conn), DbConnURI: uri}
	baseBranchName := "b0"
	repository := testCatalogerRepo(t, ctx, c, "repo", baseBranchName)

	objSkip := []int{1, 2, 3, 5, 7, 11}
	testSetupDBScannerData(t, ctx, c, repository, numberOfObjects, baseBranchName, objSkip)

	// bufferSizes - keep buffer size samples less and more then numberOfObjects
	bufferSizes := []int{1, 2, 8, 64, 512, 1024 * 4}
	for _, bufSize := range bufferSizes {
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			// test lineage scanner
			for branchNo := range objSkip {
				branchName := "b" + strconv.Itoa(branchNo)
				branchID, err := getBranchID(tx, repository, branchName, LockTypeNone)
				testutil.MustDo(t, "get branch id", err)
				scanner := NewDBLineageScanner(tx, branchID, UncommittedID, &DBScannerOptions{BufferSize: bufSize})
				for i := 0; scanner.Next(); i++ {
					o := scanner.Value()
					if o == nil {
						t.Fatal("Value() return nil, expected value")
					}
					// check item read from the right branch
					var expectedBranch int64
					for j := branchNo; j >= 0; j-- {
						if i%objSkip[j] == 0 {
							expectedBranch = int64(j + 2)
							break
						}
					}
					if o.BranchID != expectedBranch {
						t.Fatalf("Read entry with branchID=%d, expected=%d (branch=%s, number=%d)",
							o.BranchID, expectedBranch, branchName, i)
					}
				}
				v := scanner.Value()
				if v != nil {
					t.Fatalf("Value() returned %+v, expected nil after iteration", v)
				}
				testutil.MustDo(t, "next from lineage scanner", scanner.Err())
			}
			return nil, nil
		})
	}

	// get branch IDs once
	var b1BranchID, b2BranchID int64
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		var err error
		b1BranchID, err = getBranchID(tx, repository, "b1", LockTypeNone)
		testutil.MustDo(t, "get branch id", err)
		b2BranchID, err = getBranchID(tx, repository, "b2", LockTypeNone)
		testutil.MustDo(t, "get branch id", err)
		return nil, nil
	})

	// test reading committed and uncommitted data
	const bufSize = 8
	scannerOpts := &DBScannerOptions{BufferSize: bufSize, After: "Obj-0003"}
	testCatalogerCreateEntry(t, ctx, c, repository, "b1", "Obj-0004", nil, "sd1")
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageScannerB1U := NewDBLineageScanner(tx, b1BranchID, UncommittedID, scannerOpts)
		lineageScannerB1C := NewDBLineageScanner(tx, b1BranchID, CommittedID, scannerOpts)
		lineageScannerB2U := NewDBLineageScanner(tx, b2BranchID, UncommittedID, scannerOpts)
		lineageScannerB2C := NewDBLineageScanner(tx, b2BranchID, CommittedID, scannerOpts)

		const expectedMinCommit = 5
		testDBScannerNext(t, lineageScannerB1U, "read 0004 lineage b1 U", b1BranchID, MinCommitUncommittedIndicator, MaxCommitID)
		testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U", b1BranchID, expectedMinCommit, MaxCommitID)
		testDBScannerNext(t, lineageScannerB1C, "read 0004 lineage b1 C", b1BranchID, expectedMinCommit, MaxCommitID)
		testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C", b1BranchID, expectedMinCommit, MaxCommitID)
		return nil, nil
	})

	_, err := c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
	testutil.MustDo(t, "commit to b1", err)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageScannerB1U := NewDBLineageScanner(tx, b1BranchID, UncommittedID, scannerOpts)
		lineageScannerB1C := NewDBLineageScanner(tx, b1BranchID, CommittedID, scannerOpts)
		lineageScannerB2U := NewDBLineageScanner(tx, b2BranchID, UncommittedID, scannerOpts)
		lineageScannerB2C := NewDBLineageScanner(tx, b2BranchID, CommittedID, scannerOpts)
		testDBScannerNext(t, lineageScannerB1U, "read 0004 lineage b1 U", b1BranchID, 14, MaxCommitID)
		testDBScannerNext(t, lineageScannerB1C, "read 0004 lineage b1 C", b1BranchID, 14, MaxCommitID)
		testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U", b1BranchID, 5, MaxCommitID)
		testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C", b1BranchID, 5, MaxCommitID)
		return nil, nil
	})

	_, err = c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
	testutil.MustDo(t, "merge b1 into b2", err)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageScannerB2U := NewDBLineageScanner(tx, b2BranchID, UncommittedID, scannerOpts)
		lineageScannerB2C := NewDBLineageScanner(tx, b2BranchID, CommittedID, scannerOpts)
		testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b1 U", b1BranchID, 14, MaxCommitID)
		testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b1 U", b1BranchID, 14, MaxCommitID)
		return nil, nil
	})

	testutil.MustDo(t, "delete committed file on b1",
		c.DeleteEntry(ctx, repository, "b1", "Obj-0004"))
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageScannerB1U := NewDBLineageScanner(tx, b1BranchID, UncommittedID, scannerOpts)
		lineageScannerB1C := NewDBLineageScanner(tx, b1BranchID, CommittedID, scannerOpts)
		lineageScannerB2U := NewDBLineageScanner(tx, b2BranchID, UncommittedID, scannerOpts)
		lineageScannerB2C := NewDBLineageScanner(tx, b2BranchID, CommittedID, scannerOpts)
		testDBScannerNext(t, lineageScannerB1U, "read 0004 lineage b1 U", b1BranchID, MinCommitUncommittedIndicator, 0)
		testDBScannerNext(t, lineageScannerB1C, "read 0004 lineage b1 C", b1BranchID, 14, MaxCommitID)
		testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U", b1BranchID, 14, MaxCommitID)
		testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C", b1BranchID, 14, MaxCommitID)
		return nil, nil
	})

	_, err = c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
	testutil.MustDo(t, "commit to b1", err)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageScannerB1U := NewDBLineageScanner(tx, b1BranchID, UncommittedID, scannerOpts)
		lineageScannerB1C := NewDBLineageScanner(tx, b1BranchID, CommittedID, scannerOpts)
		lineageScannerB2U := NewDBLineageScanner(tx, b2BranchID, UncommittedID, scannerOpts)
		lineageScannerB2C := NewDBLineageScanner(tx, b2BranchID, CommittedID, scannerOpts)
		testDBScannerNext(t, lineageScannerB1U, "read 0004 lineage b1 U", b1BranchID, 14, 14)
		testDBScannerNext(t, lineageScannerB1C, "read 0004 lineage b1 C", b1BranchID, 14, 14)
		testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U", b1BranchID, 14, MaxCommitID)
		testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C", b1BranchID, 14, MaxCommitID)
		return nil, nil
	})

	_, err = c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
	testutil.MustDo(t, "merge b1 into b2", err)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageScannerB2U := NewDBLineageScanner(tx, b2BranchID, UncommittedID, scannerOpts)
		lineageScannerB2C := NewDBLineageScanner(tx, b2BranchID, CommittedID, scannerOpts)
		testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U", b1BranchID, 14, 14)
		testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C", b1BranchID, 14, 14)
		return nil, nil
	})

	testCatalogerCreateEntry(t, ctx, c, repository, "b1", "Obj-0004", nil, "sd2")
	_, err = c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
	testutil.MustDo(t, "commit to b1", err)
	_, err = c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
	testutil.MustDo(t, "merge b1 into b2", err)
	testutil.MustDo(t, "delete committed file on b2",
		c.DeleteEntry(ctx, repository, "b2", "Obj-0004"))
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageScannerB2U := NewDBLineageScanner(tx, b2BranchID, UncommittedID, scannerOpts)
		lineageScannerB2C := NewDBLineageScanner(tx, b2BranchID, CommittedID, scannerOpts)
		testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U", b2BranchID, MinCommitUncommittedIndicator, 0)
		testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C", b1BranchID, 18, MaxCommitID)
		return nil, nil
	})

	_, err = c.Commit(ctx, repository, "b2", "commit to b2", "tester", nil)
	testutil.MustDo(t, "commit to b1", err)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageScannerB2U := NewDBLineageScanner(tx, b2BranchID, UncommittedID, scannerOpts)
		lineageScannerB2C := NewDBLineageScanner(tx, b2BranchID, CommittedID, scannerOpts)
		testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U", b2BranchID, 20, 0)
		testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C", b2BranchID, 20, 0)
		return nil, nil
	})

	testCatalogerCreateEntry(t, ctx, c, repository, "b0", "Obj-00041", nil, "sd4")
	_, err = c.Commit(ctx, repository, "b0", "commit to b0", "tester", nil)
	testutil.MustDo(t, "commit to b0", err)
	_, err = c.Merge(ctx, repository, "b0", "b1", "tester", "", nil)
	testutil.MustDo(t, "merge b0 into b1", err)
	_, err = c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
	testutil.MustDo(t, "merge b1 into b2", err)

	testCatalogerCreateEntry(t, ctx, c, repository, "b0", "Obj-0004", nil, "sd3")
	_, err = c.Commit(ctx, repository, "b0", "commit to b0", "tester", nil)
	testutil.MustDo(t, "commit to b0", err)
	_, err = c.Merge(ctx, repository, "b0", "b1", "tester", "", nil)
	testutil.MustDo(t, "merge b0 into b1", err)
	_, err = c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
	testutil.MustDo(t, "merge b1 into b2", err)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		lineageScannerB2C := NewDBLineageScanner(tx, b2BranchID, CommittedID, scannerOpts)
		testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C", b2BranchID, 26, MaxCommitID)
		return nil, nil
	})
}

func testDBScannerNext(t *testing.T, scanner *DBLineageScanner, msg string, expBranch int64, expMinCommit CommitID, expMaxCommit CommitID) {
	t.Helper()
	if !scanner.Next() {
		testutil.MustDo(t, msg, scanner.Err())
		return
	}

	o := scanner.Value()
	if o.BranchID != expBranch {
		t.Fatalf("%s branch=%d, expected=%d",
			msg, o.BranchID, expBranch)
	}
	if o.MinCommit != expMinCommit || o.MaxCommit != expMaxCommit {
		t.Fatalf("%s min_commit=%d, expected=%d",
			msg, o.MinCommit, expMinCommit)
	}
	if o.MaxCommit != expMaxCommit {
		t.Fatalf("%s branch=%d (%d), min_commit=%d (%d), max_commit=%d (%d)",
			msg, o.BranchID, expBranch, o.MinCommit, expMinCommit, o.MaxCommit, expMaxCommit)
	}
}
