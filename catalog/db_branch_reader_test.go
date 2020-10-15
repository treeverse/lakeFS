package catalog

import (
	"context"
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

	testSetupDBReaderData(t, ctx, c, repository, numberOfObjects, maxBranchNumber, baseBranchName, objSkip)

	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		var repoID int64
		testutil.MustDo(t, "get repository id",
			tx.Get(&repoID, `SELECT id FROM catalog_repositories WHERE name=$1`, repository))

		// test different cache sizes
		for k := 0; k < len(bufferSizes); k++ {
			bufSize := bufferSizes[k]
			// test single branch reader
			for branchNo := 0; branchNo < maxBranchNumber; branchNo++ {
				branchName := "b" + strconv.Itoa(branchNo)
				var branchID int64
				testutil.MustDo(t, "get branch id",
					tx.Get(&branchID, `SELECT id FROM catalog_branches WHERE repository_id=$1 AND name=$2`, repoID, branchName))
				branchReader := NewDBBranchReader(tx, branchID, UncommittedID, bufSize, "")
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
						objNum, err := strconv.Atoi(o.Path[4:])
						testutil.MustDo(t, "convert obj number "+o.Path, err)
						if objNum != i {
							t.Errorf("objNum=%d, i=%d\n", objNum, i)
						}
					}
				}
			}
		}
		return nil, nil
	})
}
