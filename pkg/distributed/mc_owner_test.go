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
	ctx, finish := context.WithTimeout(context.Background(), time.Second)
	defer finish()
	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	if err != nil {
		t.Fatal(err)
	}
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
	ctx, finish := context.WithTimeout(context.Background(), 2*time.Second)
	defer finish()
	store, err := kv.Open(ctx, kvparams.Config{Type: "mem"})
	if err != nil {
		t.Fatal(err)
	}
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
	go func() {
		log.Info("Goroutine start")
		defer wg.Done()
		releaseB, err := w.Own(ctx, "us", "xyz")
		if err != nil {
			t.Fatalf("Own goroutine us: %s", err)
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

	if diffs := deep.Equal(events.Slice(), []string{
		"owner: me",
		"release: me",
		"owner: us",
		"released: us",
	}); diffs != nil {
		t.Errorf("Bad events ordering: diffs %s", diffs)
	}
}
