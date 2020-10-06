package catalog

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_DBBranchReader(t *testing.T) {
	const numberOfObjects = 100
	ctx := context.Background()
	conn, uri := testutil.GetDB(t, databaseURI)
	defer func() { _ = conn.Close() }()
	c := TestCataloger{Cataloger: NewCataloger(conn), DbConnURI: uri}

	const baseBranchName = "b0"
	repository := testCatalogerRepo(t, ctx, c, "repo", baseBranchName)

	t.Run("empty", func(t *testing.T) {
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			reader := NewDBBranchReader(tx, 1, UncommittedID, 64, "")
			firstNext := reader.Next()
			if firstNext {
				t.Fatalf("first entry should be false")
			}
			if reader.Err() != nil {
				t.Fatalf("first entry should not get error, got=%s", reader.Err())
			}
			return nil, nil
		})
	})

	objSkip := []int{1, 2, 3, 5, 7, 11}
	testSetupDBReaderData(t, ctx, c, repository, numberOfObjects, baseBranchName, objSkip)

	t.Run("buffer_sizes", func(t *testing.T) {
		bufferSizes := []int{1, 2, 8, 64, 512, 1024 * 4}
		for _, bufSize := range bufferSizes {
			for branchNo, objSkipNo := range objSkip {
				testDBBranchReader(t, conn, branchNo, bufSize, objSkipNo, numberOfObjects)
			}
		}
	})
}

func testDBBranchReader(t *testing.T, conn db.Database, branchNo int, bufSize int, objSkipNo int, numberOfObjects int) {
	branchID := int64(branchNo + 1)
	branchName := "b" + strconv.Itoa(branchNo)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		branchReader := NewDBBranchReader(tx, branchID, UncommittedID, bufSize, "")
		i := 0
		for branchReader.Next() {
			o := branchReader.Value()

			objNum, err := strconv.Atoi(o.Path[4:])
			testutil.MustDo(t, "convert obj number "+o.Path, err)
			if objNum != i {
				t.Errorf("objNum=%d, i=%d", objNum, i)
			}

			i += objSkipNo
		}
		testutil.MustDo(t, "read from branch "+branchName, branchReader.Err())
		if !(i-objSkipNo < numberOfObjects && i >= numberOfObjects) {
			t.Fatalf("terminated at i=%d", i)
		}
		return nil, nil
	})
}

func testSetupDBReaderData(t *testing.T, ctx context.Context, c TestCataloger, repository string, numberOfObjects int, baseBranchName string, objSkip []int) {
	for branchNo, skipCount := range objSkip {
		branchName := "b" + strconv.Itoa(branchNo)
		if branchNo > 0 {
			testCatalogerBranch(t, ctx, c, repository, branchName, baseBranchName)
		}
		for i := 0; i < numberOfObjects; i += skipCount {
			testCatalogerCreateEntry(t, ctx, c, repository, branchName, fmt.Sprintf("Obj-%04d", i), nil, "")
		}
		_, err := c.Commit(ctx, repository, branchName, "commit to "+branchName, "tester", nil)
		testutil.MustDo(t, "commit to "+branchName, err)
		baseBranchName = branchName
	}
}
