package kvtest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/kv"

	"github.com/cenkalti/backoff/v4"
)

func testTransactions(t *testing.T, ms MakeStore) {
	ctx := context.Background()
	store := ms(t, ctx)

	txnStore, ok := store.(kv.TransactionerStore)
	if !ok {
		t.Fatalf("Store %s is not a Transactioner", store)
	}

	t.Run("simple", func(t *testing.T) { testSimpleTransaction(t, ctx, txnStore) })
	t.Run("raceRetry", func(t *testing.T) { testRaceRetry(t, ctx, txnStore) })
	// TODO(ariels): Test Scan.

	// TODO(ariels): Test retries (failures) also when racing against the Store API.  (This
	// is not required to test "local", which uses the same underlying API to perform _all_
	// modifications!)
}

var (
	key1   = []byte("key1")
	value1 = []byte("the first value")
	value2 = []byte("a second value")
)

func partition(t testing.TB) []byte {
	return []byte(t.Name())
}

func tryOnce() kv.TransactionOpts {
	return kv.TransactionOpts{Backoff: backoff.WithMaxRetries(&backoff.ZeroBackOff{}, 0)}
}

func tryMany() kv.TransactionOpts {
	return kv.TransactionOpts{
		Backoff: backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(5 * time.Second)),
	}
}

// testSimpleTransaction tests a single transaction works, with no races.
func testSimpleTransaction(t testing.TB, ctx context.Context, tx kv.Transactioner) {
	// Load some data.
	err := tx.Transact(ctx, func(op kv.Operations) error {
		err := op.Set(ctx, partition(t), key1, value1)
		if err != nil {
			return err
		}

		value, err := op.Get(ctx, partition(t), key1)
		if err != nil {
			return err
		}
		if !bytes.Equal(value, value1) {
			t.Errorf("Got %s not %s on key %s", string(value), string(value1), string(key1))
		}
		return nil
	}, tryOnce())
	if err != nil {
		t.Fatalf("Transaction failed: %s", err)
	}

	// Verify it again, on another transaction.
	err = tx.Transact(ctx, func(op kv.Operations) error {
		value, err := op.Get(ctx, partition(t), key1)
		if err != nil {
			return err
		}
		if !bytes.Equal(value, value1) {
			t.Errorf("Got %s not %s on key %s", string(value), string(value1), string(key1))
		}
		return nil
	}, tryOnce())
	if err != nil {
		t.Fatalf("Transaction failed: %s", err)
	}
}

func testRaceRetry(t testing.TB, ctx context.Context, tx kv.Transactioner) {
	var (
		ch1 = make(chan struct{})
		ch2 = make(chan struct{})
		err error
	)

	go func() {
		// This transaction runs between the 2 iterations of the main transaction, and
		// always succeeds.
		err = tx.Transact(ctx, func(op kv.Operations) error {
			// Wait for the main transaction to set its value.
			<-ch1
			err = op.Set(ctx, partition(t), key1, value2)
			if err != nil {
				return err
			}
			return nil
		}, tryOnce())
		if err != nil {
			t.Errorf("Middle transaction failed: %s", err)
		}
		// Now release the main transaction, so it sees that key1 changed and tries again.
		close(ch2)
	}()

	iteration := 0
	// The main transaction.  The first time it sets a value and sleeps before reading it,
	// giving time for the auxiliary transaction above to change the value and fail it.  The
	// second time it does not need to wait, and should succeed.
	err = tx.Transact(ctx, func(op kv.Operations) error {
		// Create a dependency by reading the value.
		_, err = op.Get(ctx, partition(t), key1)
		if err != nil && !errors.Is(err, kv.ErrNotFound) {
			return err
		}
		err = op.Set(ctx, partition(t), key1, value1)
		if err != nil {
			return err
		}
		if iteration == 0 {
			// Release the auxiliary transaction.
			close(ch1)
			// Wait for it to finish.
			<-ch2
		}
		iteration++
		return nil
	}, tryMany())

	if err != nil {
		t.Error(err)
	}
	if iteration != 2 {
		t.Errorf("Main transaction ran %d != 2 times", iteration)
	}
}
