package graveler_test

import (
	"errors"
	"io"
	"testing"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/testutil"
)

func TestBranchLock(t *testing.T) {
	bl := graveler.NewBranchLocker()

	closeWrite, err := bl.AquireWrite("a", defaultBranchID)
	testutil.MustDo(t, "acquire write I", err)
	closeWrite()

	closeWrite, err = bl.AquireWrite("a", defaultBranchID)
	testutil.MustDo(t, "acquire write II", err)

	ch := make([]chan struct{}, 2)
	errs := make([]error, 2)
	doneCh := make(chan struct{})
	closeMetadataUpdateDoneCh := make(chan struct{})

	for i := 0; i < len(ch); i++ {
		ch[i] = make(chan struct{})
		errs[i] = io.EOF

		go func(id int) {
			closeMetadataUpdate, err := bl.AquireMetadataUpdate("a", defaultBranchID)
			close(ch[id])
			errs[id] = err
			if err != nil {
				return
			}
			<-doneCh
			closeMetadataUpdate()
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
	if !errors.Is(errs[failedID], graveler.ErrBranchLocked) {
		t.Fatal("one metadata update should get branch locked")
	}

	// make sure we can't write while metadata update is pending
	_, err = bl.AquireWrite("a", defaultBranchID)
	if !errors.Is(err, graveler.ErrBranchLocked) {
		t.Fatal("can't write when metadata update is pending")
	}

	// release the last writer and make sure all goroutines inside metadata update\ scope
	closeWrite()
	<-ch[pendingID]
	testutil.MustDo(t, "pending metadata update goroutine after acquire", errs[pendingID])

	// try to write again - should fail
	_, err = bl.AquireWrite("a", defaultBranchID)
	if !errors.Is(err, graveler.ErrBranchLocked) {
		t.Fatal("can't write when metadata update is running")
	}

	// release all metadata updateters and wait metadata update done
	close(doneCh)
	<-closeMetadataUpdateDoneCh

	// try to write again - should work, single writer
	closeWrite, err = bl.AquireWrite("a", defaultBranchID)
	testutil.MustDo(t, "acquire write after metadata update", err)
	closeWrite()
}
