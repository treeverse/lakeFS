package distributed

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	nanoid "github.com/matoous/go-nanoid/v2"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const mostlyCorrectOwnerPartition = "mc-ownership"

// MostlyCorrectOwner uses a Store to allow roughly at most a single goroutine to
// handle an operation for a key, across all processes sharing that store.
// It can block but never deadlock.  "Rough" ownership means that when the
// owner is too slow another owner might mistakenly be added.
//
// If:
//
//   - single ownership is not required for correctness, AND
//   - only one concurrent goroutine can succeed
//
// then using a MostlyCorrectOwner can help improve performance by usually allowing
// only one goroutine into a critical section.  This reduces retries.
//
// MostlyCorrectOwner works by setting an ownership key with timed expiration along
// with a goroutine that refreshes expiration of that key.  This can fail:
//
//   - if clocks are not synchronized
//   - if the refreshing goroutine is late
//
// So it *cannot* guarantee correctness.  However it usually works, and if
// it does work, the owning goroutine wins all races by default.
//
// MostlyCorrectOwner creates some additional load on its KV partition:
//
//   - Acquiring ownership reads at least once and writes (SetIf) once.  If
//     the key is already held, each acquisition reads once every
//     acquireInterval and once every time ownership expires.
//
//   - Holding a lock reads and writes (SetIf) once every refreshInterval.
type MostlyCorrectOwner struct {
	// acquireInterval is the polling interval for acquiring ownership.
	// Reducing it reduces some additional time to recover if an
	// instance crashes while holding ownership.  Reducing it too much
	// may cause another instance falsely to grab ownership when the
	// owner is merely slow.  Reducing it also increases read load on
	// the KV store when there is contention on the branch.
	acquireInterval time.Duration
	// refreshInterval is the time for which to assert ownership.  It
	// should be rather bigger than acquireInterval, probably at least
	// 3*.  Reducing it reduces the time to recover if an instance
	// crashes while holding ownership; because it is greater than
	// acquireInterval, it has a greater effect on this recovery time.
	// Reducing it too uch may cause another instance falsely to grab
	// ownership when the owner is merely slow.  Reducing it also
	// increases write load on the KV store, always.
	refreshInterval time.Duration

	// Log is used for logs. Everything is at a fine granularity,
	// usually TRACE.
	Log logging.Logger
	// Store is used to synchronize ownership across
	// goroutiness on multiple cooperating processes.
	Store kv.Store
	// Prefix is used to separate "locking" keys between different
	// instances of MostlyCorrectOwner.
	Prefix string
}

func NewMostlyCorrectOwner(log logging.Logger, store kv.Store, prefix string, acquireInterval, refreshInterval time.Duration) *MostlyCorrectOwner {
	return &MostlyCorrectOwner{
		acquireInterval: acquireInterval,
		refreshInterval: refreshInterval,
		Log:             log,
		Store:           store,
		Prefix:          prefix,
	}
}

// getJitter shortens interval by applying some jitter.  It will not make
// interval negative.
func getJitter(interval time.Duration) time.Duration {
	if interval < 0 {
		return 0
	}
	// Safe to use rand, jitter has no security implications.
	//nolint:mnd,gosec
	jitter := time.Duration(rand.Int63n(int64(interval) / 3))
	return interval - jitter
}

// refreshKey refreshes key for owner at interval until ctx is cancelled.
func (w *MostlyCorrectOwner) refreshKey(ctx context.Context, owner string, prefixedKey []byte) {
	// Always refresh before ownership expires.
	//nolint:mnd
	interval := w.refreshInterval / 2
	log := w.Log.WithContext(ctx).WithFields(logging.Fields{
		"interval":     interval,
		"owner":        owner,
		"prefixed_key": string(prefixedKey),
	})

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Trace("Start refreshing ownership")
	// This loop never stops the action it is supposed to protect - even
	// on error!  It is not useful to cancel ourselves, the original
	// owners.  For instance, if a new owner "stole" ownership then it
	// thought we were stuck.  Now we're back -- and we have a fair
	// chance of winning the update race.
	//
	// Cancelling anything here can lead to livelock.  It is only safe
	// to run the action to completion.
	for {
		select {
		case <-ctx.Done():
			log.Trace("Cancelled; stop refreshing ownership")
			return
		case <-ticker.C:
			ownership := MostlyCorrectOwnership{}
			predicate, err := kv.GetMsg(ctx, w.Store, mostlyCorrectOwnerPartition, prefixedKey, &ownership)
			if err != nil {
				log.WithError(err).Warn("Failed to get ownership message to refresh")
				// Do NOT attempt to delete, to avoid
				// knock-on effects destroying the new
				// owner.
				return
			}
			if ownership.Owner != owner {
				log.WithFields(logging.Fields{
					"new_owner": ownership.Owner,
				}).Info("Lost ownership race")
				return
			}
			expires := time.Now().Add(w.refreshInterval)
			ownership.Expires = timestamppb.New(expires)
			err = kv.SetMsgIf(ctx, w.Store, mostlyCorrectOwnerPartition, prefixedKey, &ownership, predicate)
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
			return ctx.Err()
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

// startOwningKey blocks until it gets mostly-correct ownership of key in
// store.  This is a spin-wait (with sleeps) because of the KV interface.
//
// TODO(ariels): Spin only once (maybe use WaitFor, or chain requests and
// spin only on the first) when multiple goroutines all wait for the same
// key.
//
// TODO(ariels): Be fair, at least in the same process.  Chaining requests
// and spinning only on the first would do this as well.
func (w *MostlyCorrectOwner) startOwningKey(ctx context.Context, owner string, prefixedKey []byte) error {
	log := w.Log.WithContext(ctx).WithFields(logging.Fields{
		"owner":            owner,
		"prefixed_key":     string(prefixedKey),
		"acquire_interval": w.acquireInterval,
		"refresh_interval": w.refreshInterval,
	})
	for {
		ownership := MostlyCorrectOwnership{}
		predicate, err := kv.GetMsg(ctx, w.Store, mostlyCorrectOwnerPartition, prefixedKey, &ownership)
		if err != nil && !errors.Is(err, kv.ErrNotFound) {
			return fmt.Errorf("start owning %s for %s: %w", prefixedKey, owner, err)
		}

		now := time.Now()
		free := checkOwnership(ownership.Expires.AsTime(), now, err)
		if free != keyOwned {
			expiryTime := now.Add(w.refreshInterval)
			ownership = MostlyCorrectOwnership{
				Owner:   owner,
				Expires: timestamppb.New(expiryTime),
				Comment: fmt.Sprintf("%s@%v", owner, now),
			}
			log.WithFields(logging.Fields{
				"ownership": free,
				"expires":   expiryTime,
				"now":       now,
			}).Trace("Try to take ownership")
			err = kv.SetMsgIf(ctx, w.Store, mostlyCorrectOwnerPartition, prefixedKey, &ownership, predicate)
			if err == nil {
				log.Trace("Got ownership")
				return nil
			}
			if !errors.Is(err, kv.ErrPredicateFailed) {
				return fmt.Errorf("start owning %s for %s: failed to set: %w",
					string(prefixedKey), owner, err)
			}
		}
		sleep := ownership.Expires.AsTime().Sub(now)
		if sleep < 0 {
			sleep = getJitter(w.acquireInterval)
		}
		log.WithField("sleep", sleep).Trace("Still owned; try again soon")
		err = sleepFor(ctx, sleep)
		if err != nil {
			return err
		}
	}
}

// releaseIf releases prefixedKey if it has the owner.
func (w *MostlyCorrectOwner) releaseIf(ctx context.Context, owner string, prefixedKey []byte) error {
	log := w.Log.WithContext(ctx).WithFields(logging.Fields{
		"prefixed_key": string(prefixedKey),
		"owner":        owner,
	})
	ownership := MostlyCorrectOwnership{}
	predicate, err := kv.GetMsg(ctx, w.Store, mostlyCorrectOwnerPartition, prefixedKey, &ownership)
	if err != nil {
		return fmt.Errorf("get ownership message %s to release it from %s: %w", string(prefixedKey), owner, err)
	}
	log = log.WithField("new_owner", ownership.Owner)

	if ownership.Owner != owner {
		log.Info("Lost ownership race before trying to release")
		return nil
	}
	// Set expiration to the beginning of time - definitely expired.
	ownership.Expires.Reset()
	err = kv.SetMsgIf(ctx, w.Store, mostlyCorrectOwnerPartition, prefixedKey, &ownership, predicate)
	if errors.Is(err, kv.ErrPredicateFailed) {
		log.WithFields(logging.Fields{
			"prefixed_key": string(prefixedKey),
			"owner":        owner,
			"new_owner":    ownership.Owner,
		}).Info("Lost ownership race while trying to release")
		// Succeeded: we did not
		return nil
	}
	return err
}

// Own blocks until it gets mostly-correct ownership of key for owner.
// Ownership will be refreshed at resolution interval.  It returns a
// function to stop owning key.  Own appends its random slug to owner, to
// identify the owner uniquely.
func (w *MostlyCorrectOwner) Own(ctx context.Context, owner, key string) (func(), error) {
	owner = fmt.Sprintf("%s#%s", owner, nanoid.Must())
	prefixedKey := []byte(fmt.Sprintf("%s/%s", w.Prefix, key))
	err := w.startOwningKey(ctx, owner, prefixedKey)
	if err != nil {
		return nil, fmt.Errorf("start owning %s for %s: %w", owner, key, err)
	}
	refreshCtx, refreshCancel := context.WithCancel(ctx)
	go w.refreshKey(refreshCtx, owner, prefixedKey)
	stopOwning := func() {
		defer refreshCancel()
		// This func might be called twice, so use the original ctx not refreshCtx.
		err := w.releaseIf(ctx, owner, prefixedKey)
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
