package graveler_test

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/testutil"
	tu "github.com/treeverse/lakefs/testutil"
)

func TestBranchLock(t *testing.T) {
	t.Skip("re-implement with new interface")
	conn, _ := tu.GetDB(t, databaseURI)
	bl := graveler.NewBranchLocker(conn)

	ctx := context.Background()
	_, err := bl.Writer(ctx, "a", testutil.DefaultBranchID, func() (interface{}, error) {
		return nil, nil
	})
	tu.MustDo(t, "acquire write I", err)
	//closeWrite()

	_, err = bl.Writer(ctx, "a", testutil.DefaultBranchID, func() (interface{}, error) {
		return nil, nil
	})
	tu.MustDo(t, "acquire write II", err)

	ch := make([]chan struct{}, 2)
	errs := make([]chan error, 2)
	doneCh := make(chan struct{})
	closeMetadataUpdateDoneCh := make(chan struct{})

	for i := 0; i < len(ch); i++ {
		ch[i] = make(chan struct{})
		errs[i] = make(chan error, 1)
		go func(id int) {
			_, err := bl.MetadataUpdater(ctx, "a", testutil.DefaultBranchID, func() (interface{}, error) {
				return nil, nil
			})
			close(ch[id])
			errs[id] <- err
			if err != nil {
				return
			}
			<-doneCh
			//closeMetadataUpdate()
			close(closeMetadataUpdateDoneCh)
		}(i)
	}

	failedID := -1
	pendingID := -1
	select {
	case <-ch[0]:
		failedID, pendingID = 0, 1
	case <-ch[1]:
		failedID, pendingID = 1, 0
	}
	err = <-errs[failedID]
	if !errors.Is(err, graveler.ErrBranchLocked) {
		t.Fatal("one metadata update should get branch locked")
	}

	// make sure we can't write while metadata update is pending
	_, err = bl.Writer(ctx, "a", testutil.DefaultBranchID, func() (interface{}, error) {
		return nil, nil
	})
	if !errors.Is(err, graveler.ErrBranchLocked) {
		t.Fatal("can't write when metadata update is pending")
	}

	// release the last writer and make sure all goroutines inside metadata update\ scope
	//closeWrite()
	<-ch[pendingID]
	err = <-errs[pendingID]
	tu.MustDo(t, "pending metadata update goroutine after acquire", err)

	// try to write again - should fail
	_, err = bl.Writer(ctx, "a", testutil.DefaultBranchID, func() (interface{}, error) {
		return nil, nil
	})
	if !errors.Is(err, graveler.ErrBranchLocked) {
		t.Fatal("can't write when metadata update is running")
	}

	// release all metadata updates and wait metadata update done
	close(doneCh)
	<-closeMetadataUpdateDoneCh

	// try to write again - should work, single writer
	_, err = bl.Writer(ctx, "a", testutil.DefaultBranchID, func() (interface{}, error) {
		return nil, nil
	})
	tu.MustDo(t, "acquire write after metadata update", err)
	//closeWrite()
}
