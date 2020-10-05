package catalog

import (
	"context"
	"testing"

	"github.com/treeverse/lakefs/db"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_DBReadEntry(t *testing.T) {
	const numberOfObjects = 10

	ctx := context.Background()
	conn, uri := testutil.GetDB(t, databaseURI)
	defer conn.Close()
	c := TestCataloger{Cataloger: NewCataloger(conn), DbConnURI: uri}
	baseBranchName := "b0"
	repository := testCatalogerRepo(t, ctx, c, "repo", baseBranchName)
	objSkip := []int{1, 2, 3, 5, 7, 11}
	maxBranchNumber := len(objSkip)

	testSetupDBReaderData(t, ctx, c, repository, numberOfObjects, maxBranchNumber, baseBranchName, objSkip)
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		_ = LineageSelect(4, []string{"qaz"}, 0, tx)
		_ = LineageSelect(4, []string{"qaz", "uiu", "uyug"}, 0, tx)
		return nil, nil
	})

}

//func testSetupDBReaderData(t *testing.T, ctx context.Context, c TestCataloger, repository string, numberOfObjects int, maxBranchNumber int, baseBranchName string, objSkip []int) {
//	for branchNo := 0; branchNo < maxBranchNumber; branchNo++ {
//		branchName := "b" + strconv.Itoa(branchNo)
//		if branchNo > 0 {
//			testCatalogerBranch(t, ctx, c, repository, branchName, baseBranchName)
//		}
//		for i := 0; i < numberOfObjects; i += objSkip[branchNo] {
//			testCatalogerCreateEntry(t, ctx, c, repository, branchName, fmt.Sprintf("Obj-%04d", i), nil, "")
//		}
//		_, err := c.Commit(ctx, repository, branchName, "commit to "+branchName, "tester", nil)
//		testutil.MustDo(t, "commit to "+branchName, err)
//		baseBranchName = branchName
//	}
//}
