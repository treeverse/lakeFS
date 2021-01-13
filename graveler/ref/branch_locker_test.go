package ref_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/ref"
	"github.com/treeverse/lakefs/graveler/testutil"
	tu "github.com/treeverse/lakefs/testutil"
)

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

	t.Run("committer_blocks_all", func(t *testing.T) {
		chAcquired := make(chan struct{})
		chReleaseAcquired := make(chan struct{})
		chDone := make(chan struct{})
		go func() {
			ctx := context.Background()
			_, err := bl.MetadataUpdater(ctx, "b", testutil.DefaultBranchID, func() (interface{}, error) {
				close(chAcquired)
				<-chReleaseAcquired
				return nil, nil
			})
			if err != nil {
				t.Error("Metadata updater request failed:", err)
			}
			close(chDone)
		}()

		ctx := context.Background()
		// wait until we acquire metadata update lock
		<-chAcquired
		// try to acquire writer
		_, err := bl.Writer(ctx, "b", testutil.DefaultBranchID, func() (interface{}, error) {
			return nil, nil
		})
		if !errors.Is(err, graveler.ErrLockNotAcquired) {
			t.Fatalf("Writer should be locked during metadata updater, err=%v", err)
		}
		// try to acquire committer
		_, err = bl.MetadataUpdater(ctx, "b", testutil.DefaultBranchID, func() (interface{}, error) {
			return nil, nil
		})
		if !errors.Is(err, graveler.ErrLockNotAcquired) {
			t.Fatalf("Committer should be locked during metadata updater, err=%s", err)
		}
		// release acquired lock
		close(chReleaseAcquired)
		<-chDone
		// check we can write
		_, err = bl.Writer(ctx, "b", testutil.DefaultBranchID, func() (interface{}, error) {
			return nil, nil
		})
		if err != nil {
			t.Fatalf("Failed to acquire writer, err=%s", err)
		}
	})

	t.Run("committer_wait_for_writers", func(t *testing.T) {
		// start a writer and block it
		chAcquireWriter := make(chan struct{})
		chReleaseWriter := make(chan struct{})
		chDoneWriter := make(chan struct{})
		go func() {
			ctx := context.Background()
			_, err := bl.Writer(ctx, "c", testutil.DefaultBranchID, func() (interface{}, error) {
				close(chAcquireWriter)
				<-chReleaseWriter
				return nil, nil
			})
			if err != nil {
				t.Errorf("Failed to acquire writer: %s", err)
			}
			close(chDoneWriter)
		}()
		<-chAcquireWriter

		// start two committers and wait until one of them will fail to know that the first one is blocked
		var metadataUpdates int64
		chDoneCommitter := make([]chan struct{}, 2)
		committersErr := make([]error, 2)
		for i := 0; i < len(chDoneCommitter); i++ {
			chDoneCommitter[i] = make(chan struct{})
			go func(pos int) {
				ctx := context.Background()
				_, committersErr[pos] = bl.MetadataUpdater(ctx, "c", testutil.DefaultBranchID, func() (interface{}, error) {
					atomic.AddInt64(&metadataUpdates, 1)
					return nil, nil
				})
				close(chDoneCommitter[pos])
			}(i)
		}

		// wait for one of the committers
		var failedCommitterPos int
		select {
		case <-chDoneCommitter[0]:
			failedCommitterPos = 0
		case <-chDoneCommitter[1]:
			failedCommitterPos = 1
		}

		// check that the one that failed - failed with the right reason
		if err := committersErr[failedCommitterPos]; !errors.Is(err, graveler.ErrLockNotAcquired) {
			t.Fatalf("Failed committer (%d) should failed to acquire, err=%v", failedCommitterPos, err)
		}
		updates := atomic.LoadInt64(&metadataUpdates)
		if updates != 0 {
			t.Fatalf("No update should be done at this point, updates=%d", updates)
		}

		// verify that another writer can't start while committer is waiting
		ctx := context.Background()
		_, err := bl.Writer(ctx, "c", testutil.DefaultBranchID, func() (interface{}, error) {
			return nil, nil
		})
		if !errors.Is(err, graveler.ErrLockNotAcquired) {
			t.Fatalf("Should not acquire writer while committer is waiting, err=%v", err)
		}

		// release the last writer and wait for it
		close(chReleaseWriter)
		<-chDoneWriter

		// wait for the second committer
		secondCommitterPos := (failedCommitterPos + 1) % 2
		<-chDoneCommitter[secondCommitterPos]

		// verify no error and one update
		if err := committersErr[secondCommitterPos]; err != nil {
			t.Fatalf("Committer should ended without an error, err=%v", err)
		}
		updates = atomic.LoadInt64(&metadataUpdates)
		if updates != 1 {
			t.Fatalf("Expected one update, updates=%d", updates)
		}
	})
}
