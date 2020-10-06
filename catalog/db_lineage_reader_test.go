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
	defer func() { _ = conn.Close() }()

	c := TestCataloger{Cataloger: NewCataloger(conn), DbConnURI: uri}
	baseBranchName := "b0"
	repository := testCatalogerRepo(t, ctx, c, "repo", baseBranchName)

	objSkip := []int{1, 2, 3, 5, 7, 11}
	testSetupDBReaderData(t, ctx, c, repository, numberOfObjects, baseBranchName, objSkip)

	bufferSizes := []int{1, 2, 8, 64, 512, 1024 * 4}
	t.Run("buffer_sizes", func(t *testing.T) {
		for _, bufSize := range bufferSizes {
			_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
				// test lineage reader
				for branchNo := range objSkip {
					branchName := "b" + strconv.Itoa(branchNo)
					branchID := int64(branchNo + 1)
					lineageReader := NewDBLineageReader(tx, branchID, UncommittedID, bufSize, "")
					for i := 0; i < numberOfObjects; i++ {
						o, err := lineageReader.Next()
						position := fmt.Sprintf("branch=%s, number=%d", branchName, i)
						testutil.MustDo(t, "next from lineage reader "+position, err)
						if o == nil {
							t.Error("Got nil obj", position)
						}
						// check item read from might branch
						var expectedBranch int64
						for j := branchNo; j >= 0; j-- {
							if i%objSkip[j] == 0 {
								expectedBranch = int64(j + 1)
								break
							}
						}
						if o.BranchID != expectedBranch {
							t.Fatalf("fetch branchID=%d, expected=%d (%s)", o.BranchID, expectedBranch, position)
						}
					}
				}
				return nil, nil
			})
		}
	})

	// test reading committed and uncommitted data
	const bufSize = 8
	t.Run("uncommitted", func(t *testing.T) {
		testCatalogerCreateEntry(t, ctx, c, repository, "b1", "Obj-0004", nil, "sd1")
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB1U := NewDBLineageReader(tx, 2, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB1C := NewDBLineageReader(tx, 2, CommittedID, bufSize, "Obj-0003")
			lineageReaderB2U := NewDBLineageReader(tx, 3, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB2C := NewDBLineageReader(tx, 3, CommittedID, bufSize, "Obj-0003")
			testDBReaderNext(t, lineageReaderB1U, "read 0004 lineage b1 U ", 2, MinCommitUncommittedIndicator, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 2, 4, MaxCommitID)
			testDBReaderNext(t, lineageReaderB1C, "read 0004 lineage b1 C ", 2, 4, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 2, 4, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("committed", func(t *testing.T) {
		_, err := c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB1U := NewDBLineageReader(tx, 2, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB1C := NewDBLineageReader(tx, 2, CommittedID, bufSize, "Obj-0003")
			lineageReaderB2U := NewDBLineageReader(tx, 3, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB2C := NewDBLineageReader(tx, 3, CommittedID, bufSize, "Obj-0003")
			testDBReaderNext(t, lineageReaderB1U, "read 0004 lineage b1 U ", 2, 13, MaxCommitID)
			testDBReaderNext(t, lineageReaderB1C, "read 0004 lineage b1 C ", 2, 13, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 2, 4, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 2, 4, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("merged", func(t *testing.T) {
		_, err := c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
		testutil.MustDo(t, "merge b1 into b2", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB2U := NewDBLineageReader(tx, 3, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB2C := NewDBLineageReader(tx, 3, CommittedID, bufSize, "Obj-0003")
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b1 U ", 2, 13, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b1 U ", 2, 13, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("delete_uncommitted", func(t *testing.T) {
		testutil.MustDo(t, "delete committed file on b1",
			c.DeleteEntry(ctx, repository, "b1", "Obj-0004"))
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB1U := NewDBLineageReader(tx, 2, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB1C := NewDBLineageReader(tx, 2, CommittedID, bufSize, "Obj-0003")
			lineageReaderB2U := NewDBLineageReader(tx, 3, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB2C := NewDBLineageReader(tx, 3, CommittedID, bufSize, "Obj-0003")
			testDBReaderNext(t, lineageReaderB1U, "read 0004 lineage b1 U ", 2, MinCommitUncommittedIndicator, 0)
			testDBReaderNext(t, lineageReaderB1C, "read 0004 lineage b1 C ", 2, 13, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 2, 13, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 2, 13, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("delete_committed", func(t *testing.T) {
		_, err := c.Commit(ctx, repository, "b1", "commit to b1", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB1U := NewDBLineageReader(tx, 2, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB1C := NewDBLineageReader(tx, 2, CommittedID, bufSize, "Obj-0003")
			lineageReaderB2U := NewDBLineageReader(tx, 3, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB2C := NewDBLineageReader(tx, 3, CommittedID, bufSize, "Obj-0003")
			testDBReaderNext(t, lineageReaderB1U, "read 0004 lineage b1 U ", 2, 13, 13)
			testDBReaderNext(t, lineageReaderB1C, "read 0004 lineage b1 C ", 2, 13, 13)
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 2, 13, MaxCommitID)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 2, 13, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("merge", func(t *testing.T) {
		_, err := c.Merge(ctx, repository, "b1", "b2", "tester", "", nil)
		testutil.MustDo(t, "merge b1 into b2", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB2U := NewDBLineageReader(tx, 3, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB2C := NewDBLineageReader(tx, 3, CommittedID, bufSize, "Obj-0003")
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 2, 13, 13)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 2, 13, 13)
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
			lineageReaderB2U := NewDBLineageReader(tx, 3, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB2C := NewDBLineageReader(tx, 3, CommittedID, bufSize, "Obj-0003")
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 3, MinCommitUncommittedIndicator, 0)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 2, 17, MaxCommitID)
			return nil, nil
		})
	})

	t.Run("merge", func(t *testing.T) {
		_, err := c.Commit(ctx, repository, "b2", "commit to b2", "tester", nil)
		testutil.MustDo(t, "commit to b1", err)
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			lineageReaderB2U := NewDBLineageReader(tx, 3, UncommittedID, bufSize, "Obj-0003")
			lineageReaderB2C := NewDBLineageReader(tx, 3, CommittedID, bufSize, "Obj-0003")
			testDBReaderNext(t, lineageReaderB2U, "read 0004 lineage b2 U ", 3, 19, 0)
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 3, 19, 0)
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
			lineageReaderB2C := NewDBLineageReader(tx, 3, CommittedID, bufSize, "Obj-0003")
			testDBReaderNext(t, lineageReaderB2C, "read 0004 lineage b2 C ", 3, 25, MaxCommitID)
			return nil, nil
		})
	})
}

func testDBReaderNext(t *testing.T, reader *DBLineageReader, msg string, expBranch, expMinCommit CommitID, expMaxCommit CommitID) {
	o, err := reader.Next()
	testutil.MustDo(t, msg, err)
	if o.BranchID != int64(expBranch) || o.MinCommit != expMinCommit || o.MaxCommit != expMaxCommit {
		t.Errorf("%s branch=%d (%d), min_commit=%d (%d), max_commit=%d (%d)",
			msg, o.BranchID, expBranch, o.MinCommit, expMinCommit, o.MaxCommit, expMaxCommit)
	}
}
