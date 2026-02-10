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

func TestInProcessKeyedLock_HandoffToCancelledWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		l := distributed.NewInProcessKeyedLock()

		events := Ordering[string]{}

		r1, err := l.Acquire(ctx, "k")
		if err != nil {
			t.Fatalf("Acquire holder: %v", err)
		}

		cancelCtx, cancelA := context.WithCancel(ctx)
		var wg sync.WaitGroup

		// A waits (will be cancelled concurrently with handoff).
		wg.Go(func() {
			release, err := l.Acquire(cancelCtx, "k")
			if err != nil {
				events.Add("A:cancelled")
				return
			}
			events.Add("A:acquired")
			release()
		})
		synctest.Wait()

		// B waits.
		wg.Go(func() {
			release, err := l.Acquire(ctx, "k")
			if err != nil {
				t.Errorf("B Acquire: %v", err)
				return
			}
			events.Add("B:acquired")
			release()
		})
		synctest.Wait()

		// Release the holder (hands off to A by closing A's w.ch)
		// AND cancel A's context, both before A has a chance to run.
		// This puts A in the state where both w.ch and ctx.Done() are
		// ready: if the runtime select picks ctx.Done(), A must detect
		// the handoff in the inner select and propagate to B.
		r1()
		cancelA()
		wg.Wait()

		// Regardless of which select branch A takes, B must acquire.
		got := events.Slice()
		foundB := false
		for _, e := range got {
			if e == "B:acquired" {
				foundB = true
			}
		}
		if !foundB {
			t.Errorf("B never acquired the lock; events: %v", got)
		}
	})
}

func TestInProcessKeyedLock_Stress(t *testing.T) {
	l := distributed.NewInProcessKeyedLock()
	const (
		workers = 20
		iters   = 100
		numKeys = 3
	)

	// Per-key counters protected only by the keyed lock (not a mutex).
	// The race detector will catch any violation.
	type counter struct{ n int }
	counters := make([]*counter, numKeys)
	for i := range counters {
		counters[i] = &counter{}
	}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range iters {
				ki := w % numKeys
				key := fmt.Sprintf("key-%d", ki)

				var (
					ctx    = context.Background()
					cancel context.CancelFunc
				)
				if i%7 == 0 {
					ctx, cancel = context.WithTimeout(ctx, time.Millisecond)
				}

				release, err := l.Acquire(ctx, key)
				if cancel != nil {
					cancel()
				}
				if err != nil {
					continue
				}
				counters[ki].n++
				release()
			}
		})
	}
	wg.Wait()

	total := 0
	for _, c := range counters {
		total += c.n
	}
	if total == 0 {
		t.Fatal("no acquisitions succeeded")
	}
	t.Logf("completed %d/%d acquisitions across %d keys", total, workers*iters, numKeys)
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
