package graveler_test

import (
	"testing"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/testutil"
)

func TestBranchLocker(t *testing.T) {
	bl := graveler.NewBranchLocker()
	closeWrite, err := bl.AquireWrite("a", defaultBranchID)
	testutil.MustDo(t, "aquire write I", err)
	closeWrite()

	closeWrite, err = bl.AquireWrite("a", defaultBranchID)
	testutil.MustDo(t, "aquire write II", err)

	writeWhileCommitEnded := make(chan struct{})
	commitCalled := make(chan struct{})
	commitStarted := make(chan struct{})
	commitEnded := make(chan struct{})

	go func() {
		close(commitCalled)
		closeCommit, err := bl.AquireMetadataUpdate("a", defaultBranchID)
		close(commitStarted)
		if err != nil {
			t.Error(err)
		}
		<-writeWhileCommitEnded
		closeCommit()
		close(commitEnded)
	}()
	<-commitCalled
	// try to commit while commit is pending
	_, err = bl.AquireWrite("a", defaultBranchID)
	if err == nil {
		t.Fatal("expected can't after write")
	}
	closeWrite()
	<-commitStarted
	// try to commit while commit is running
	_, err = bl.AquireWrite("a", defaultBranchID)
	if err == nil {
		t.Fatal("expected can't after write")
	}
	close(writeWhileCommitEnded)
	<-commitEnded
	// write after commit ended
	closeWrite, err = bl.AquireWrite("a", defaultBranchID)
	if err != nil {
		t.Fatal("expected can write")
	}
	closeWrite()
}
