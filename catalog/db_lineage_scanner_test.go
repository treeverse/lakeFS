package catalog

import (
	"context"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_DBLineageScanner(t *testing.T) {
	const numberOfObjects = 10

	ctx := context.Background()
	conn, uri := testutil.GetDB(t, databaseURI)
	defer func() { _ = conn.Close() }()

	c := TestCataloger{Cataloger: NewCataloger(conn), DbConnURI: uri}
	baseBranchName := "b0"
	repository := testCatalogerRepo(t, ctx, c, "repo", baseBranchName)

	objSkip := []int{1, 2, 3, 5, 7, 11}
	testSetupDBScannerData(t, ctx, c, repository, numberOfObjects, baseBranchName, objSkip)

	bufferSizes := []int{1, 2, 8, 64, 512, 1024 * 4}
	t.Run("buffer_sizes", func(t *testing.T) {
		for _, bufSize := range bufferSizes {
			_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
				// test lineage scanner
				for branchNo := range objSkip {
					branchName := "b" + strconv.Itoa(branchNo)
					branchID := int64(branchNo + 1)
					scanner := NewDBLineageScanner(tx, branchID, UncommittedID, &DBScannerOptions{BufferSize: bufSize})
					for i := 0; scanner.Next(); i++ {
						o := scanner.Value()
						// check item read from might branch
						var expectedBranch int64
						for j := branchNo; j >= 0; j-- {
							if i%objSkip[j] == 0 {
								expectedBranch = int64(j + 1)
								break
							}
						}
						if o.BranchID != expectedBranch {
							t.Fatalf("fetch branchID=%d, expected=%d (branch=%s, number=%d)",
								o.BranchID, expectedBranch, branchName, i)
						}
					}
					testutil.MustDo(t, "next from lineage scanner", scanner.Err())
				}
				return nil, nil
			})
		}
	})

	// test reading committed and uncommitted data
	const bufSize = 8
	scannerOpts := &DBScannerOptions{BufferSize: bufSize, After: "Obj-0003"}
	t.Run("uncommitted", func(t *testing.T) {
		testCatalogerCreateEntry(t, ctx, c, repository, "b1", "Obj-0004", nil, "sd1")
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageScannerB1U := NewDBLineageScanner(tx, 2, UncommittedID, scannerOpts)
			lineageScannerB1C := NewDBLineageScanner(tx, 2, CommittedID, scannerOpts)
			lineageScannerB2U := NewDBLineageScanner(tx, 3, UncommittedID, scannerOpts)
			lineageScannerB2C := NewDBLineageScanner(tx, 3, CommittedID, scannerOpts)
			testDBScannerNext(t, lineageScannerB1U, "read 0004 lineage b1 U ", 2, MinCommitUncommittedIndicator, MaxCommitID)
			testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U ", 2, 4, MaxCommitID)
			testDBScannerNext(t, lineageScannerB1C, "read 0004 lineage b1 C ", 2, 4, MaxCommitID)
			testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C ", 2, 4, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("committed", func(t *testing.T) {
		_, err := c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageScannerB1U := NewDBLineageScanner(tx, 2, UncommittedID, scannerOpts)
			lineageScannerB1C := NewDBLineageScanner(tx, 2, CommittedID, scannerOpts)
			lineageScannerB2U := NewDBLineageScanner(tx, 3, UncommittedID, scannerOpts)
			lineageScannerB2C := NewDBLineageScanner(tx, 3, CommittedID, scannerOpts)
			testDBScannerNext(t, lineageScannerB1U, "read 0004 lineage b1 U ", 2, 13, MaxCommitID)
			testDBScannerNext(t, lineageScannerB1C, "read 0004 lineage b1 C ", 2, 13, MaxCommitID)
			testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U ", 2, 4, MaxCommitID)
			testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C ", 2, 4, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("merged", func(t *testing.T) {
		_, err := c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
		testutil.MustDo(t, "merge b1 into b2", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageScannerB2U := NewDBLineageScanner(tx, 3, UncommittedID, scannerOpts)
			lineageScannerB2C := NewDBLineageScanner(tx, 3, CommittedID, scannerOpts)
			testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b1 U ", 2, 13, MaxCommitID)
			testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b1 U ", 2, 13, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("delete_uncommitted", func(t *testing.T) {
		testutil.MustDo(t, "delete committed file on b1",
			c.DeleteEntry(ctx, repository, "b1", "Obj-0004"))
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageScannerB1U := NewDBLineageScanner(tx, 2, UncommittedID, scannerOpts)
			lineageScannerB1C := NewDBLineageScanner(tx, 2, CommittedID, scannerOpts)
			lineageScannerB2U := NewDBLineageScanner(tx, 3, UncommittedID, scannerOpts)
			lineageScannerB2C := NewDBLineageScanner(tx, 3, CommittedID, scannerOpts)
			testDBScannerNext(t, lineageScannerB1U, "read 0004 lineage b1 U ", 2, MinCommitUncommittedIndicator, 0)
			testDBScannerNext(t, lineageScannerB1C, "read 0004 lineage b1 C ", 2, 13, MaxCommitID)
			testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U ", 2, 13, MaxCommitID)
			testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C ", 2, 13, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("delete_committed", func(t *testing.T) {
		_, err := c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageScannerB1U := NewDBLineageScanner(tx, 2, UncommittedID, scannerOpts)
			lineageScannerB1C := NewDBLineageScanner(tx, 2, CommittedID, scannerOpts)
			lineageScannerB2U := NewDBLineageScanner(tx, 3, UncommittedID, scannerOpts)
			lineageScannerB2C := NewDBLineageScanner(tx, 3, CommittedID, scannerOpts)
			testDBScannerNext(t, lineageScannerB1U, "read 0004 lineage b1 U ", 2, 13, 13)
			testDBScannerNext(t, lineageScannerB1C, "read 0004 lineage b1 C ", 2, 13, 13)
			testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U ", 2, 13, MaxCommitID)
			testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C ", 2, 13, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("merge", func(t *testing.T) {
		_, err := c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
		testutil.MustDo(t, "merge b1 into b2", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageScannerB2U := NewDBLineageScanner(tx, 3, UncommittedID, scannerOpts)
			lineageScannerB2C := NewDBLineageScanner(tx, 3, CommittedID, scannerOpts)
			testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U ", 2, 13, 13)
			testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C ", 2, 13, 13)
			return nil, nil
		})
	})

	t.Run("merge", func(t *testing.T) {
		testCatalogerCreateEntry(t, ctx, c, repository, "b1", "Obj-0004", nil, "sd2")
		_, err := c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, err = c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
		testutil.MustDo(t, "merge b1 into b2", err)
		testutil.MustDo(t, "delete committed file on b2",
			c.DeleteEntry(ctx, repository, "b2", "Obj-0004"))
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageScannerB2U := NewDBLineageScanner(tx, 3, UncommittedID, scannerOpts)
			lineageScannerB2C := NewDBLineageScanner(tx, 3, CommittedID, scannerOpts)
			testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U ", 3, MinCommitUncommittedIndicator, 0)
			testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C ", 2, 17, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("merge", func(t *testing.T) {
		_, err := c.Commit(ctx, repository, "b2", "commit to b2", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageScannerB2U := NewDBLineageScanner(tx, 3, UncommittedID, scannerOpts)
			lineageScannerB2C := NewDBLineageScanner(tx, 3, CommittedID, scannerOpts)
			testDBScannerNext(t, lineageScannerB2U, "read 0004 lineage b2 U ", 3, 19, 0)
			testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C ", 3, 19, 0)
			return nil, nil
		})
	})

	t.Run("merge", func(t *testing.T) {
		testCatalogerCreateEntry(t, ctx, c, repository, "b0", "Obj-00041", nil, "sd4")
		_, err := c.Commit(ctx, repository, "b0", "commit to b0", "tester", nil)
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
			lineageScannerB2C := NewDBLineageScanner(tx, 3, CommittedID, scannerOpts)
			testDBScannerNext(t, lineageScannerB2C, "read 0004 lineage b2 C ", 3, 25, MaxCommitID)
			return nil, nil
		})
	})
}

func testDBScannerNext(t *testing.T, scanner *DBLineageScanner, msg string, expBranch, expMinCommit CommitID, expMaxCommit CommitID) {
	if !scanner.Next() {
		testutil.MustDo(t, msg, scanner.Err())
		return
	}

	o := scanner.Value()
	if o.BranchID != int64(expBranch) || o.MinCommit != expMinCommit || o.MaxCommit != expMaxCommit {
		t.Errorf("%s branch=%d (%d), min_commit=%d (%d), max_commit=%d (%d)",
			msg, o.BranchID, expBranch, o.MinCommit, expMinCommit, o.MaxCommit, expMaxCommit)
	}
}
