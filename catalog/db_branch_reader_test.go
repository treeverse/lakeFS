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
	defer conn.Close()
	c := TestCataloger{Cataloger: NewCataloger(conn), DbConnURI: uri}
	baseBranchName := "b0"
	repository := testCatalogerRepo(t, ctx, c, "repo", baseBranchName)
	objSkip := []int{1, 2, 3, 5, 7, 11}
	bufferSizes := []int{1, 2, 8, 64, 512, 1024 * 4}
	maxBranchNumber := len(objSkip)

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

	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		var p string
		// test different cache sizes
		for k := 0; k < len(bufferSizes); k++ {
			bufSize := bufferSizes[k]
			// test single branch reader
			for branchNo := 0; branchNo < maxBranchNumber; branchNo++ {
				branchName := "b" + strconv.Itoa(branchNo)
				branchReader := NewDBBranchReader(tx, int64(branchNo+1), UncommittedID, bufSize, "")
				objSkipNo := objSkip[branchNo]
				for i := 0; ; i += objSkipNo {
					o, err := branchReader.Next()
					testutil.MustDo(t, "read from branch "+branchName, err)
					if o == nil {
						if !(i-objSkipNo < numberOfObjects && i >= numberOfObjects) {
							t.Fatalf("terminated at i=%d", i)
						}
						break
					} else {
						p = o.Path
						objNum, err := strconv.Atoi(p[4:])
						testutil.MustDo(t, "convert obj number "+p, err)
						if objNum != i {
							t.Errorf(" objNum=%d, i=%d\n", objNum, i)
						}
					}
				}
			}
		}
		return nil, nil
	})
}
