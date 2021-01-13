package graveler

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v4"

	"github.com/treeverse/lakefs/db"
)

// BranchLocker enforces the branch locking logic
// The logic is as follows:
// - Allow concurrent writers to acquire the lock.
// - A Metadata update waits for all current writers to release the lock, and then gets the lock.
// - While a metadata update has the lock or is waiting for the lock, any other operation fails to acquire the lock.
type BranchLocker struct {
	db db.Database
}

func NewBranchLocker(db db.Database) *BranchLocker {
	return &BranchLocker{
		db: db,
	}
}

// Writer tries to acquire a write lock using a Postgres advisory lock for the span of calling `lockedCB`.
// Returns ErrLockNotAcquired if it cannot acquire the lock or if a commit is in progress.
func (l *BranchLocker) Writer(ctx context.Context, repositoryID RepositoryID, branchID BranchID, lockedCB func() (interface{}, error)) (interface{}, error) {
	writerLockKey, _ := calculateBranchLockerKeys(repositoryID, branchID)
	return l.db.Transact(func(tx db.Tx) (interface{}, error) {
		// try lock committer key
		var locked bool
		err := tx.GetPrimitive(&locked, `SELECT pg_try_advisory_xact_lock_shared($1)`, writerLockKey)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", ErrLockNotAcquired, writerLockKey, err)
		}
		if !locked {
			return nil, fmt.Errorf("%w (%d)", ErrLockNotAcquired, writerLockKey)
		}
		return lockedCB()
	}, db.WithContext(ctx), db.WithIsolationLevel(pgx.ReadCommitted))
}

// MetadataUpdater tries to lock as committer using a Postgres advisory lock for the span of calling `lockedCB`.
// The call is blocked until all writers end their work.
// It returns ErrLockNotAcquired if it fails to acquire the lock or if another commit is already in progress.
func (l *BranchLocker) MetadataUpdater(ctx context.Context, repositoryID RepositoryID, branchID BranchID, lockedCB func() (interface{}, error)) (interface{}, error) {
	writerLockKey, committerLockKey := calculateBranchLockerKeys(repositoryID, branchID)
	return l.db.Transact(func(tx db.Tx) (interface{}, error) {
		// try lock committer key
		var locked bool
		err := tx.GetPrimitive(&locked, `SELECT pg_try_advisory_xact_lock($1);`, committerLockKey)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", ErrLockNotAcquired, committerLockKey, err)
		}
		if !locked {
			return nil, fmt.Errorf("%w (%d)", ErrLockNotAcquired, writerLockKey)
		}
		// lock writer key
		_, err = tx.Exec(`SELECT pg_advisory_xact_lock($1);`, writerLockKey)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", ErrLockNotAcquired, writerLockKey, err)
		}
		return lockedCB()
	}, db.WithContext(ctx), db.WithIsolationLevel(pgx.ReadCommitted))
}

func calculateBranchLockerKeys(repositoryID RepositoryID, branchID BranchID) (writerLockKey int64, committerLockKey int64) {
	h := fnv.New64()
	_, _ = h.Write([]byte(repositoryID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(branchID))
	_, _ = h.Write([]byte{0})
	writerLockKey = int64(h.Sum64())
	committerLockKey = writerLockKey + 1
	return
}
