package catalog

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_DBLineageReader(t *testing.T) {
	const numberOfObjects = 10

	ctx := context.Background()
	conn, uri := testutil.GetDB(t, databaseURI)
	defer conn.Close()
	c := TestCataloger{Cataloger: NewCataloger(conn), DbConnURI: uri}
	baseBranchName := "b0"
	repository := testCatalogerRepo(t, ctx, c, "repo", baseBranchName)
	objSkip := []int{1, 2, 3, 5, 7, 11}
	bufferSizes := []int{1, 2, 8, 64, 512, 1024 * 4}
	maxBranchNumber := len(objSkip)

	testSetupDBReaderData(t, ctx, c, repository, numberOfObjects, maxBranchNumber, baseBranchName, objSkip)

	t.Run("cache_sizes", func(t *testing.T) {
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			// test different cache sizes
			for k := 0; k < len(bufferSizes); k++ {
				bufSize := bufferSizes[k]
				// test lineage reader
				for branchNo := 0; branchNo < maxBranchNumber; branchNo++ {
					branchName := "b" + strconv.Itoa(branchNo)
					lineageReader, err := NewDBLineageReader(tx, int64(branchNo+2), UncommittedID, bufSize, -1, "")
					testutil.MustDo(t, "new lineage reader "+branchName, err)
					for i := 0; i < numberOfObjects; i++ {
						var expectedBranch int64
						o, err := lineageReader.Next()
						testutil.MustDo(t, "read from lineage "+branchName, err)
						if o == nil {
							t.Errorf("Got nil obj, branch=%s, number=%d", branchName, i)
						}
						// check item read from might branch
						for j := branchNo; j >= 0; j-- {
							if i%objSkip[j] == 0 {
								expectedBranch = int64(j + 2)
								break
							}
						}
						if o.BranchID != expectedBranch {
							t.Errorf("fetch from wrong branch.branchName=%s branchNumber=%d, i =%d\n", branchName, o.BranchID, i)
						}
					}
				}
			}
			return nil, nil
		})
	})

	// test reading committed and uncommitted data
	bufSize := 8
	t.Run("uncommitted", func(t *testing.T) {
		testCatalogerCreateEntry(t, ctx, c, repository, "b1", "Obj-0004", nil, "sd1")
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB1U, err := NewDBLineageReader(tx, 3, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(3), err)
			lineageReaderB1C, err := NewDBLineageReader(tx, 3, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(3), err)
			lineageReaderB2U, err := NewDBLineageReader(tx, 4, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			lineageReaderB2C, err := NewDBLineageReader(tx, 4, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			testDBReaderNext(t, lineageReaderB1U, "read 0004 lineage b1 U ", 3, MinCommitUncommittedIndicator, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 3, 5, MaxCommitID)
			testDBReaderNext(t, lineageReaderB1C, "read 0004 lineage b1 C ", 3, 5, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 3, 5, MaxCommitID)
			return nil, nil
		})
	})
	t.Run("committed", func(t *testing.T) {
		_, err := c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB1U, err := NewDBLineageReader(tx, 3, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(3), err)
			lineageReaderB1C, err := NewDBLineageReader(tx, 3, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(3), err)
			lineageReaderB2U, err := NewDBLineageReader(tx, 4, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			lineageReaderB2C, err := NewDBLineageReader(tx, 4, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			testDBReaderNext(t, lineageReaderB1U, "read 0004 lineage b1 U ", 3, 14, MaxCommitID)
			testDBReaderNext(t, lineageReaderB1C, "read 0004 lineage b1 C ", 3, 14, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 3, 5, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 3, 5, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("merged", func(t *testing.T) {
		_, err := c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
		testutil.MustDo(t, "merge b1 into b2", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB2U, err := NewDBLineageReader(tx, 4, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			lineageReaderB2C, err := NewDBLineageReader(tx, 4, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b1 U ", 3, 14, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b1 U ", 3, 14, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("delete_uncommitted", func(t *testing.T) {
		testutil.MustDo(t, "delete committed file on b1",
			c.DeleteEntry(ctx, repository, "b1", "Obj-0004"))
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB1U, err := NewDBLineageReader(tx, 3, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(3), err)
			lineageReaderB1C, err := NewDBLineageReader(tx, 3, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(3), err)
			lineageReaderB2U, err := NewDBLineageReader(tx, 4, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			lineageReaderB2C, err := NewDBLineageReader(tx, 4, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			testDBReaderNext(t, lineageReaderB1U, "read 0004 lineage b1 U ", 3, MinCommitUncommittedIndicator, 0)
			testDBReaderNext(t, lineageReaderB1C, "read 0004 lineage b1 C ", 3, 14, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 3, 14, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 3, 14, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("delete_committed", func(t *testing.T) {
		_, err := c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB1U, err := NewDBLineageReader(tx, 3, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(3), err)
			lineageReaderB1C, err := NewDBLineageReader(tx, 3, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(3), err)
			lineageReaderB2U, err := NewDBLineageReader(tx, 4, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			lineageReaderB2C, err := NewDBLineageReader(tx, 4, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			testDBReaderNext(t, lineageReaderB1U, "read 0004 lineage b1 U ", 3, 14, 14)
			testDBReaderNext(t, lineageReaderB1C, "read 0004 lineage b1 C ", 3, 14, 14)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 3, 14, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 3, 14, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("merge", func(t *testing.T) {
		_, err := c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
		testutil.MustDo(t, "merge b1 into b2", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB2U, err := NewDBLineageReader(tx, 4, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			lineageReaderB2C, err := NewDBLineageReader(tx, 4, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 3, 14, 14)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 3, 14, 14)
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
			lineageReaderB2U, err := NewDBLineageReader(tx, 4, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			lineageReaderB2C, err := NewDBLineageReader(tx, 4, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 4, MinCommitUncommittedIndicator, 0)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 3, 18, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("merge", func(t *testing.T) {
		_, err := c.Commit(ctx, repository, "b2", "commit to b2", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB2U, err := NewDBLineageReader(tx, 4, UncommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			lineageReaderB2C, err := NewDBLineageReader(tx, 4, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 4, 20, 0)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 4, 20, 0)
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
			lineageReaderB2C, err := NewDBLineageReader(tx, 4, CommittedID, bufSize, -1, "Obj-0003")
			testutil.MustDo(t, "new lineage reader branchID="+strconv.Itoa(4), err)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 4, 26, MaxCommitID)
			return nil, nil
		})
	})
}

func testSetupDBReaderData(t *testing.T, ctx context.Context, c TestCataloger, repository string, numberOfObjects int, maxBranchNumber int, baseBranchName string, objSkip []int) {
	for branchNo := 0; branchNo < maxBranchNumber; branchNo++ {
		branchName := "b" + strconv.Itoa(branchNo)
		if branchNo > 0 {
			testCatalogerBranch(t, ctx, c, repository, branchName, baseBranchName)
		}
		for i := 0; i < numberOfObjects; i += objSkip[branchNo] {
			testCatalogerCreateEntry(t, ctx, c, repository, branchName, fmt.Sprintf("Obj-%04d", i), nil, "")
		}
		_, err := c.Commit(ctx, repository, branchName, "commit to "+branchName, "tester", nil)
		testutil.MustDo(t, "commit to "+branchName, err)
		baseBranchName = branchName
	}
}

func testDBReaderNext(t *testing.T, lReader *DBLineageReader, msg string, expBranch, expMinCommit CommitID, expMaxCommit CommitID) {
	t.Helper()
	o, err := lReader.Next()
	testutil.MustDo(t, msg, err)
	if o.BranchID != int64(expBranch) || o.MinCommit != CommitID(expMinCommit) || o.MaxCommit != expMaxCommit {
		t.Errorf("%s branch=%d (%d), min_commit=%d (%d), max_commit=%d (%d)", msg, o.BranchID, expBranch, o.MinCommit, expMinCommit, o.MaxCommit, expMaxCommit)
	}
}
