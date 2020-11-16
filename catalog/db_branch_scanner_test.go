package catalog

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	sq "github.com/Masterminds/squirrel"

	"github.com/davecgh/go-spew/spew"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/testutil"
)

func TestDBBranchScanner(t *testing.T) {
	const numberOfObjects = 100
	ctx := context.Background()
	conn, uri := testutil.GetDB(t, databaseURI)
	c := TestCataloger{Cataloger: NewCataloger(conn), DbConnURI: uri}

	const baseBranchName = "b0"
	repository := testCatalogerRepo(t, ctx, c, "repo", baseBranchName)

	// get branch ID once
	var branchID int64
	_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
		var err error
		branchID, err = getBranchID(tx, repository, "b0", LockTypeNone)
		testutil.MustDo(t, "get branch id", err)
		return nil, nil
	})

	t.Run("empty", func(t *testing.T) {
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			scanner := NewDBBranchScanner(tx, branchID, UncommittedID, 0, DBScannerOptions{BufferSize: 64})
			firstNext := scanner.Next()
			if firstNext {
				t.Fatalf("first entry should be false")
			}
			if scanner.Err() != nil {
				t.Fatalf("first entry should not get error, got=%s", scanner.Err())
			}
			v := scanner.Value()
			if v != nil {
				t.Fatalf("Scanner Value=%+v, expected nil", v)
			}
			return nil, nil
		})
	})

	objSkip := []int{1, 2, 3, 5, 7, 11}
	testSetupDBScannerData(t, ctx, c, repository, numberOfObjects, baseBranchName, objSkip)

	t.Run("additional_fields", func(t *testing.T) {
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			scanner := NewDBBranchScanner(tx, branchID, UncommittedID, 0, DBScannerOptions{
				AdditionalFields: []string{"checksum", "physical_address"},
			})
			testedSomething := false
			for scanner.Next() {
				ent := scanner.Value()
				if ent.Checksum == "" {
					t.Fatalf("Entry missing value for Checksum: %s", spew.Sdump(ent))
				}
				if ent.PhysicalAddress == "" {
					t.Fatalf("Entry missing value for PhysicalAddress: %s", spew.Sdump(ent))
				}
				if !testedSomething {
					testedSomething = true
				}
			}
			testutil.MustDo(t, "read from branch for additional fields", scanner.Err())
			if !testedSomething {
				t.Fatal("Not tested something with additional fields")
			}
			return nil, nil
		})
	})

	t.Run("additional_where", func(t *testing.T) {
		_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
			p := fmt.Sprintf("Obj-%04d", numberOfObjects-5)
			scanner := NewDBBranchScanner(tx, branchID, UncommittedID, 0, DBScannerOptions{
				AdditionalWhere: sq.Expr("path=?", p),
			})
			var ent *DBScannerEntry
			for scanner.Next() {
				ent = scanner.Value()
			}
			testutil.MustDo(t, "read from branch for additional fields", scanner.Err())
			if ent == nil {
				t.Fatal("missing entry")
			} else if ent.Path != p {
				t.Fatalf("Read entry with path=%s, expected=%s", ent.Path, p)
			}
			return nil, nil
		})
	})

	t.Run("buffer_sizes", func(t *testing.T) {
		bufferSizes := []int{1, 2, 3, 8, 19, 64, 512}
		for _, bufSize := range bufferSizes {
			for branchNo, objSkipNo := range objSkip {
				branchName := "b" + strconv.Itoa(branchNo)
				_, _ = conn.Transact(func(tx db.Tx) (interface{}, error) {
					branchID, err := getBranchID(tx, repository, branchName, LockTypeNone)
					testutil.MustDo(t, "get branch ID", err)
					scanner := NewDBBranchScanner(tx, branchID, UncommittedID, 0, DBScannerOptions{BufferSize: bufSize})
					i := 0
					for scanner.Next() {
						o := scanner.Value()

						objNum, err := strconv.Atoi(o.Path[4:])
						testutil.MustDo(t, "convert obj number "+o.Path, err)
						if objNum != i {
							t.Errorf("objNum=%d, i=%d", objNum, i)
						}

						i += objSkipNo
					}
					testutil.MustDo(t, "read from branch "+branchName, scanner.Err())
					if !(i-objSkipNo < numberOfObjects && i >= numberOfObjects) {
						t.Fatalf("terminated at i=%d", i)
					}
					return nil, nil
				})
			}
		}
	})
}

func testSetupDBScannerData(t *testing.T, ctx context.Context, c TestCataloger, repository string, numberOfObjects int, baseBranchName string, objSkip []int) {
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
