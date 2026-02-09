package distributed_test

// The Go race detector does not help test MostlyCorrectOwner: a store like
// mem must protect its data structures from races.  So every operation
// synchronizes with every other operation - the race detector cannot detect
// races!
//
// Meanwhile mocks can help test MostlyCorrectOwner behaviour when errors occur, but
// not that it actually behaves correctly.
//
// Instead, observe behaviour and test that.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-test/deep"

	"github.com/treeverse/lakefs/pkg/distributed"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	_ "github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/logging"
)

// TestMostlyCorrectOwnerSingleThreaded tests behaviour with a single owner.
func TestMostlyCorrectOwnerSingleThreaded(t *testing.T) {
	// Fail quickly on deadlock
	ctx, finish := context.WithTimeout(t.Context(), time.Second)
	defer finish()
	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := logging.FromContext(ctx).WithField("test", t.Name())

	w := distributed.NewMostlyCorrectOwner(log, store, "p", 5*time.Millisecond, 10*time.Millisecond)

	releaseAbc, err := w.Own(ctx, "me", "abc")
	if err != nil {
		t.Fatalf("Failed to own \"abc\": %s", err)
	}
	defer releaseAbc()

	releaseXyz, err := w.Own(ctx, "me", "xyz")
	if err != nil {
		t.Fatalf("Failed to own \"xyz\": %s", err)
	}
	defer releaseXyz()

	// Got here, so different keys did not deadlock.
}

// Ordering retains an ordering on events without creating a data race.
type Ordering[T any] struct {
	n      atomic.Int32
	values sync.Map
}

func (o *Ordering[T]) Add(value T) {
	key := o.n.Add(int32(1)) - 1
	o.values.Store(key, value)
}

// Slice returns a snapshot of all Adds
func (o *Ordering[T]) Slice() []T {
	ret := make([]T, 0, o.n.Load())
	o.values.Range(func(k, v any) bool {
		key := k.(int32)
		value := v.(T)
		if len(ret) <= int(key) {
			// Add was called between Load and Range, but
			// probably not too many times.
			var zero T
			for i := int32(len(ret)); i < key; i++ {
				ret = append(ret, zero)
			}
			ret = append(ret, value)
		} else {
			ret[key] = value
		}
		return true // keep going
	})
	return ret
}

// TestMostlyCorrectOwnerConsecutive tests behaviour with a single owner - second
// Own() call should wait until the first is released.  But Own() never
// times out on its own.
func TestMostlyCorrectOwnerConsecutiveReleased(t *testing.T) {
	// Fail quickly on deadlock
	ctx, finish := context.WithTimeout(t.Context(), 2*time.Second)
	defer finish()
	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := logging.FromContext(ctx).WithField("test", t.Name())

	w := distributed.NewMostlyCorrectOwner(log, store, "p", 5*time.Millisecond, 40*time.Millisecond)
	events := Ordering[string]{}

	releaseA, err := w.Own(ctx, "me", "xyz")
	if err != nil {
		t.Fatalf("Own main me: %s", err)
	}
	events.Add("owner: me")

	wg := sync.WaitGroup{}

	wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		log.Info("Goroutine start")
		defer wg.Done()
		releaseB, err := w.Own(ctx, "us", "xyz")
		if err != nil {
			errChan <- fmt.Errorf("Own goroutine us: %s", err)
			return
		}
		defer func() {
			releaseB()
			events.Add("released: us")
		}()
		events.Add("owner: us")
	}()

	time.Sleep(150 * time.Millisecond)
	events.Add("release: me")
	releaseA()
	wg.Wait()

	select {
	case err := <-errChan:
		t.Fatal(err)
	default:
	}

	if diffs := deep.Equal(events.Slice(), []string{
		"owner: me",
		"release: me",
		"owner: us",
		"released: us",
	}); diffs != nil {
		t.Errorf("Bad events ordering: diffs %s", diffs)
	}
}

func TestMostlyCorrectOwnerConsecutiveCancelled(t *testing.T) {
	ctx, finish := context.WithTimeout(t.Context(), 2*time.Second)
	defer finish()
	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := logging.FromContext(ctx).WithField("test", t.Name())

	w := distributed.NewMostlyCorrectOwner(log, store, "p", 5*time.Millisecond, 40*time.Millisecond)
	releaseA, err := w.Own(ctx, "me", "xyz")
	if err != nil {
		t.Fatalf("Own main me: %s", err)
	}
	defer releaseA()

	waitCtx, waitCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer waitCancel()
	_, err = w.Own(waitCtx, "us", "xyz")
	if err == nil {
		t.Fatal("expected cancellation error while waiting for ownership")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("Own should fail with deadline exceeded, got: %v", err)
	}
}

// TestMostlyCorrectOwnerFIFOOrdering verifies that multiple waiters on the
// same key are granted ownership in FIFO order.
func TestMostlyCorrectOwnerFIFOOrdering(t *testing.T) {
	ctx, finish := context.WithTimeout(t.Context(), 5*time.Second)
	defer finish()
	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	log := logging.FromContext(ctx).WithField("test", t.Name())

	w := distributed.NewMostlyCorrectOwner(log, store, "p", 5*time.Millisecond, 40*time.Millisecond)
	events := Ordering[string]{}

	// First owner holds the key.
	releaseA, err := w.Own(ctx, "A", "key")
	if err != nil {
		t.Fatalf("Own A: %s", err)
	}

	const n = 3
	ready := make([]chan struct{}, n)
	for i := range ready {
		ready[i] = make(chan struct{})
	}

	var wg sync.WaitGroup
	errChan := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Signal that this goroutine is about to call Own.
			close(ready[idx])
			release, err := w.Own(ctx, fmt.Sprintf("W%d", idx), "key")
			if err != nil {
				errChan <- fmt.Errorf("Own W%d: %w", idx, err)
				return
			}
			events.Add(fmt.Sprintf("owner: W%d", idx))
			release()
		}(i)
		// Wait for the goroutine to be ready before launching the next,
		// so they enqueue in order W0, W1, W2.
		<-ready[i]
		// Give the goroutine time to actually block inside Acquire.
		time.Sleep(10 * time.Millisecond)
	}

	// Release the first owner — waiters should proceed in FIFO order.
	releaseA()
	wg.Wait()
	close(errChan)
	for err := range errChan {
		t.Fatal(err)
	}

	expected := []string{"owner: W0", "owner: W1", "owner: W2"}
	if diffs := deep.Equal(events.Slice(), expected); diffs != nil {
		t.Errorf("Expected FIFO ordering %v, got %v (diffs: %s)", expected, events.Slice(), diffs)
	}
}
