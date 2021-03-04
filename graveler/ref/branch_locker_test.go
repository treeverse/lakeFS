package ref_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/ref"
	"github.com/treeverse/lakefs/graveler/testutil"
	tu "github.com/treeverse/lakefs/testutil"
)

var errUnexpectedCall = errors.New("this function should not have been called")

func TestBranchLock(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	bl := ref.NewBranchLocker(conn)

	t.Run("multiple_writers", func(t *testing.T) {
		const rounds = 10
		for round := 0; round < rounds; round++ {
			const writers = 5
			var writerCounter int64
			var wgDone sync.WaitGroup
			wgDone.Add(writers)
			for i := 0; i < writers; i++ {
				go func() {
					defer wgDone.Done()
					ctx := context.Background()
					_, err := bl.Writer(ctx, "repo-writers", testutil.DefaultBranchID, func() (interface{}, error) {
						runtime.Gosched()
						atomic.AddInt64(&writerCounter, 1)
						return nil, nil
					})
					if err != nil {
						t.Errorf("Failed to acquire writer, err=%s", err)
					}
				}()
			}
			wgDone.Wait()
			// verify all writers worked
			counter := atomic.LoadInt64(&writerCounter)
			if counter != writers {
				t.Fatalf("Writer worked %d, expected %d", counter, writers)
			}
		}
	})

	t.Run("committer_blocks_writer", func(t *testing.T) {
		chReleaseAcquired := make(chan struct{})
		chAcquired := make(chan struct{})
		defer close(chReleaseAcquired)
		// call writer and wait on channel
		ctx := context.Background()
		go func() {
			_, err := bl.MetadataUpdater(ctx, "committer_blocks_writer", testutil.DefaultBranchID, func() (interface{}, error) {
				close(chAcquired)
				<-chReleaseAcquired
				return nil, nil
			})
			if err != nil {
				t.Error("Metadata updater request failed:", err)
			}
		}()
		<-chAcquired
		// check Writer waits (context gets to deadline before Writer callback is called)
		timeToDeadline := time.Now().Add(time.Second)
		ctxWithDeadline, _ := context.WithDeadline(ctx, timeToDeadline)
		_, err := bl.Writer(ctxWithDeadline, "committer_blocks_writer", testutil.DefaultBranchID, func() (interface{}, error) {
			return nil, errUnexpectedCall
		})
		if !errors.Is(err, graveler.ErrLockNotAcquired) {
			t.Errorf("unexpected error got: %v expected: %s", err, graveler.ErrLockNotAcquired)
		}
	})

	t.Run("writer_blocks_commiter", func(t *testing.T) {
		chReleaseAcquired := make(chan struct{})
		defer close(chReleaseAcquired)
		chAcquired := make(chan struct{})
		// call writer and wait on channel
		ctx := context.Background()
		go func() {
			_, err := bl.Writer(ctx, "writer_blocks_commiter", testutil.DefaultBranchID, func() (interface{}, error) {
				close(chAcquired)
				<-chReleaseAcquired
				return nil, nil
			})
			if err != nil {
				t.Error("Metadata updater request failed:", err)
			}
		}()
		<-chAcquired
		// check MetadataUpdater waits ( context gets to deadline before MetadataUpdater callback is called)
		timeToDeadline := time.Now().Add(time.Second)
		ctxWithDeadline, cancel := context.WithDeadline(ctx, timeToDeadline)
		defer cancel()

		_, err := bl.MetadataUpdater(ctxWithDeadline, "writer_blocks_commiter", testutil.DefaultBranchID, func() (interface{}, error) {
			return nil, errUnexpectedCall
		})
		if !errors.Is(err, graveler.ErrLockNotAcquired) {
			t.Errorf("unexpected error got: %v expected: %s", err, graveler.ErrLockNotAcquired)
		}
	})
}

// TestBranchLockPanic panic during metadata updater, checks that MetadataUpdater releases the lock
// calling the method twice will locked in case of an error
func TestBranchLockPanic(t *testing.T) {
	conn, _ := tu.GetDB(t, databaseURI)
	bl := ref.NewBranchLocker(conn)
	panicOnMetadataUpdate(bl)
	panicOnMetadataUpdate(bl)
}

func panicOnMetadataUpdate(bl *ref.BranchLocker) {
	chDone := make(chan struct{})
	go func() {
		// ignore panics and release the function call
		defer func() {
			recover()
			close(chDone)
		}()
		ctx := context.Background()
		_, _ = bl.MetadataUpdater(ctx, "branch-locker", testutil.DefaultBranchID, func() (interface{}, error) {
			panic("metadata updater")
		})
	}()
	<-chDone
}
