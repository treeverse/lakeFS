package ref_test

import (
	"context"
	"errors"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
	tu "github.com/treeverse/lakefs/pkg/testutil"
	"testing"
)

var errUnexpectedCall = errors.New("this function should not have been called")

// TestBranchLockPanic panic during metadata updater, checks that MetadataUpdater releases the lock
// calling the method twice will lock in case of an error
func TestBranchLockPanic(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	bl := ref.NewBranchLocker(conn)
	panicOnMetadataUpdate(bl)
	panicOnMetadataUpdate(bl)
}

func panicOnMetadataUpdate(bl *ref.BranchLocker) {
	repository := &graveler.RepositoryRecord{
		RepositoryID: "branch-locker",
		Repository:   nil,
	}
	chDone := make(chan struct{})
	go func() {
		// ignore panics and release the function call
		defer func() {
			_ = recover()
			close(chDone)
		}()
		ctx := context.Background()
		_, _ = bl.MetadataUpdater(ctx, repository, testutil.DefaultBranchID, func() (interface{}, error) {
			panic("metadata updater")
		})
	}()
	<-chDone
}
