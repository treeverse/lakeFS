package util

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const weakOwnerPartition = "weak-ownership"

var finished = errors.New("finished")

// A WeakOwner uses a Store to allow roughly at most a single goroutine to
// handle an operation for a key, across all processes sharing that store.
// It can block but never deadlock.  "Rough" ownership means that when the
// owner is too slow another owner might mistakenly be added.
//
// If:
//
//   - single ownership is not required for correctness, AND
//   - only one concurrent goroutine can succeed
//
// then using a WeakOwner can help improve performance by usually allowing
// only one goroutine into a critical section.  This reduces retries.
//
// WeakOwner works by setting an ownership key with timed expiration along
// with a goroutine that refreshes expiration of that key.  This can fail:
//
//   - if clocks are not synchronized
//   - if the refreshing goroutine is late
//
// So it *cannot* guarantee correctness.  However it usually works, and if
// it does work, the owning goroutine wins all races by default.
type WeakOwner struct {
	// Log is used for logs. Everything is at a fine granularity,
	// usually TRACE.
	Log logging.Logger
	// Store is used to synchronize ownership across
	// goroutiness on multiple cooperating processes.
	Store kv.Store
	// Prefix is used to select "locking" keys.
	Prefix string
}

func NewWeakOwner(log logging.Logger, store kv.Store, prefix string) *WeakOwner {
	return &WeakOwner{Log: log, Store: store, Prefix: prefix}
}

// refreshKey refreshes key for owner at interval until ctx is cancelled.
func (w *WeakOwner) refreshKey(ctx context.Context, owner string, prefixedKey []byte, interval time.Duration) {
	log := w.Log.WithContext(ctx).WithFields(logging.Fields{
		"interval":     interval,
		"owner":        owner,
		"prefixed_key": string(prefixedKey),
	})

	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()
	log.Trace("Start refreshing ownership")
	for {
		select {
		case <-ctx.Done():
			log.Trace("Cancelled; stop refreshing ownership")
			return
		case <-ticker.C:
			ownership := WeakOwnership{}
			predicate, err := kv.GetMsg(ctx, w.Store, weakOwnerPartition, prefixedKey, &ownership)
			if err != nil {
				log.WithError(err).Warn("Failed to get ownership message to refresh")
				// Do NOT attempt to delete, to avoid
				// knock-on effects destroying the new
				// owner.
				break
			}
			if ownership.Owner != owner {
				log.WithFields(logging.Fields{
					"new_owner": ownership.Owner,
				}).Info("Lost ownership race")
				break
			}
			expires := time.Now().Add(interval)
			ownership.Expires = timestamppb.New(expires)
			err = kv.SetMsgIf(ctx, w.Store, weakOwnerPartition, prefixedKey, &ownership, predicate)
			if err != nil {
				log.WithError(err).Warn("Failed to set ownership message to refresh (keep going, may lose)")
				continue
			}
			log.WithField("expires", expires).Trace("Refreshed ownership")
		}
	}
}

// sleepFor sleeps for duration or until ctx is Done.  If Done, it returns
// the error from ctx or finished.
//
// TODO(ariels): Rewrite once on Go 1.23, which no longer leaks time.After.
func sleepFor(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			err := ctx.Err()
			if err == nil {
				err = finished
			}
			return err
		}
	}
}

type keyOwnership string

const (
	keyOwned    keyOwnership = "owned"
	keyNotOwned keyOwnership = "not owned"
	keyExpired  keyOwnership = "expired"
)

func checkOwnership(expires, now time.Time, getErr error) keyOwnership {
	switch {
	case errors.Is(getErr, kv.ErrNotFound):
		return keyNotOwned
	case expires.Before(now):
		return keyExpired
	default:
		return keyOwned
	}
}

// startOwningKey blocks until it gets weak ownership of key in store.
// This is a spin-wait (with sleeps) because of the KV interface.
//
// TODO(ariels): Spin only once (maybe use WaitFor, or chain requests and
// spin only on the first) when multiple goroutines all wait for the same
// key.
//
// TODO(ariels): Be fair, at least in the same process.  Chaining requests
// and spinning only on the first would do this as well.
func (w *WeakOwner) startOwningKey(ctx context.Context, owner string, prefixedKey []byte, acquireInterval, refreshInterval time.Duration) error {
	log := w.Log.WithContext(ctx).WithFields(logging.Fields{
		"owner":            owner,
		"prefixed_key":     string(prefixedKey),
		"acquire_interval": acquireInterval,
		"refresh_interval": refreshInterval,
	})
	for {
		ownership := WeakOwnership{}
		predicate, err := kv.GetMsg(ctx, w.Store, weakOwnerPartition, prefixedKey, &ownership)
		if err != nil && !errors.Is(err, kv.ErrNotFound) {
			return fmt.Errorf("start owning %s for %s: %w", prefixedKey, owner, err)
		}

		now := time.Now()
		free := checkOwnership(ownership.Expires.AsTime(), now, err)
		if free != keyOwned {
			expiryTime := now.Add(refreshInterval)
			ownership = WeakOwnership{
				Owner:   owner,
				Expires: timestamppb.New(expiryTime),
				Comment: fmt.Sprintf("%s@%v", owner, now),
			}
			log.WithFields(logging.Fields{
				"ownership": free,
				"expires":   expiryTime,
				"now":       now,
			}).Trace("Try to take ownership")
			if errors.Is(err, kv.ErrNotFound) {
				err = kv.SetMsg(ctx, w.Store, weakOwnerPartition, prefixedKey, &ownership)
			} else {
				err = kv.SetMsgIf(ctx, w.Store, weakOwnerPartition, prefixedKey, &ownership, predicate)
			}
			if err == nil {
				log.Trace("Got ownership")
				return nil
			}
			if !errors.Is(err, kv.ErrPredicateFailed) {
				return fmt.Errorf("start owning %s for %s: failed to set: %w",
					string(prefixedKey), owner, err)
			}
		}
		sleep := ownership.Expires.AsTime().Sub(now) - 5*time.Millisecond
		if sleep < 0 {
			sleep = acquireInterval
		}
		log.WithField("sleep", sleep).Trace("Still owned; try again soon")
		err = sleepFor(ctx, sleep)
		if err != nil {
			return err
		}
	}
}

// Own blocks until it gets weak ownership of key for owner.  Ownership
// will be refreshed at resolution interval.  It returns a function to stop
// owning key.
func (w *WeakOwner) Own(ctx context.Context, owner, key string, acquireInterval, refreshInterval time.Duration) (func(), error) {
	prefixedKey := []byte(fmt.Sprintf("%s/%s", w.Prefix, key))
	err := w.startOwningKey(context.Background(), owner, prefixedKey, acquireInterval, refreshInterval)
	if err != nil {
		return nil, fmt.Errorf("start owning %s for %s: %w", owner, key, err)
	}
	refreshCtx, refreshCancel := context.WithCancel(ctx)
	go w.refreshKey(refreshCtx, owner, prefixedKey, refreshInterval/2)
	stopOwning := func() {
		defer refreshCancel()
		// Use the original context - in case cancelled twice.
		err := w.Store.Delete(ctx, []byte(weakOwnerPartition), prefixedKey)
		if err != nil {
			w.Log.
				WithContext(ctx).
				WithError(err).
				WithFields(logging.Fields{
					"owner": owner,
					"key":   key,
				}).
				Warn("Failed to delete ownership")
		}
		w.Log.
			WithContext(ctx).
			WithFields(logging.Fields{
				"owner": owner,
				"key":   key,
			}).
			Trace("Deleted ownership")
	}
	return stopOwning, nil
}
