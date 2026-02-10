package distributed_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/distributed"
)

func TestInProcessKeyedLock_NoContention(t *testing.T) {
	ctx := t.Context()
	l := distributed.NewInProcessKeyedLock()

	release, err := l.Acquire(ctx, "a")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	release()
}

func TestInProcessKeyedLock_DifferentKeysNoBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	l := distributed.NewInProcessKeyedLock()

	r1, err := l.Acquire(ctx, "a")
	if err != nil {
		t.Fatalf("Acquire a: %v", err)
	}
	defer r1()

	r2, err := l.Acquire(ctx, "b")
	if err != nil {
		t.Fatalf("Acquire b: %v", err)
	}
	defer r2()
}

func TestInProcessKeyedLock_ContextCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := distributed.NewInProcessKeyedLock()
		ctx := t.Context()

		r1, err := l.Acquire(ctx, "k")
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		defer r1()

		waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer waitCancel()

		_, err = l.Acquire(waitCtx, "k")
		if err == nil {
			t.Fatal("expected error from cancelled context")
		}
		if err != context.DeadlineExceeded {
			t.Fatalf("expected DeadlineExceeded, got: %v", err)
		}
	})
}

func TestInProcessKeyedLock_FIFOOrdering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		l := distributed.NewInProcessKeyedLock()

		events := Ordering[string]{}

		r1, err := l.Acquire(ctx, "k")
		if err != nil {
			t.Fatalf("Acquire first: %v", err)
		}

		const n = 5
		var wg sync.WaitGroup
		for idx := range n {
			wg.Go(func() {
				release, err := l.Acquire(ctx, "k")
				if err != nil {
					t.Errorf("Acquire W%d: %v", idx, err)
					return
				}
				events.Add(fmt.Sprintf("W%d", idx))
				release()
			})
			synctest.Wait()
		}

		r1()
		wg.Wait()

		expected := []string{"W0", "W1", "W2", "W3", "W4"}
		if diffs := deep.Equal(events.Slice(), expected); diffs != nil {
			t.Errorf("Expected FIFO %v, got %v (diffs: %s)", expected, events.Slice(), diffs)
		}
	})
}

func TestInProcessKeyedLock_CancelledMiddleWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		l := distributed.NewInProcessKeyedLock()

		events := Ordering[string]{}

		r1, err := l.Acquire(ctx, "k")
		if err != nil {
			t.Fatalf("Acquire first: %v", err)
		}

		// Enqueue 3 waiters: W0, W1 (will cancel), W2
		var wg sync.WaitGroup
		cancelCtx, cancelW1 := context.WithCancel(ctx)

		for i := 0; i < 3; i++ {
			wg.Add(1)
			acquireCtx := ctx
			if i == 1 {
				acquireCtx = cancelCtx
			}
			go func(idx int, acquireCtx context.Context) {
				defer wg.Done()
				release, err := l.Acquire(acquireCtx, "k")
				if err != nil {
					events.Add(fmt.Sprintf("W%d:cancelled", idx))
					return
				}
				events.Add(fmt.Sprintf("W%d:acquired", idx))
				release()
			}(i, acquireCtx)
			synctest.Wait()
		}

		// Cancel W1 before releasing the first holder.
		cancelW1()
		synctest.Wait()

		r1()
		wg.Wait()

		// W1 should have bailed; W0 and W2 proceed in order.
		expected := []string{"W1:cancelled", "W0:acquired", "W2:acquired"}
		if diffs := deep.Equal(events.Slice(), expected); diffs != nil {
			t.Errorf("Expected %v, got %v (diffs: %s)", expected, events.Slice(), diffs)
		}
	})
}

func TestInProcessKeyedLock_ReleaseAndReacquire(t *testing.T) {
	ctx := t.Context()
	l := distributed.NewInProcessKeyedLock()

	release, err := l.Acquire(ctx, "k")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Release must be called exactly once.
	release()

	// Should be able to acquire again after release.
	r2, err := l.Acquire(ctx, "k")
	if err != nil {
		t.Fatalf("Re-acquire: %v", err)
	}
	r2()
}
