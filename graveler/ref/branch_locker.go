package ref

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v4"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
)

type BranchLocker struct {
	db db.Database
}

func NewBranchLocker(db db.Database) *BranchLocker {
	return &BranchLocker{
		db: db,
	}
}

// Writer tries to acquire a write lock using a Postgres advisory lock for the span of calling `lockedFn`.
// Returns ErrLockNotAcquired if it cannot acquire the lock or if a commit is in progress.
func (l *BranchLocker) Writer(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, lockedFn graveler.BranchLockerFunc) (interface{}, error) {
	writerLockKey, _ := calculateBranchLockerKeys(repositoryID, branchID)
	return l.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		// try to get a shared lock on the writer key
		var locked bool
		err := tx.GetPrimitive(&locked, `SELECT pg_try_advisory_xact_lock_shared($1)`, writerLockKey)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", graveler.ErrLockNotAcquired, writerLockKey, err)
		}
		if !locked {
			return nil, fmt.Errorf("%w (%d)", graveler.ErrAlreadyLocked, writerLockKey)
		}
		return lockedFn()
	}, db.WithIsolationLevel(pgx.ReadCommitted))
}

// MetadataUpdater tries to lock as committer using a Postgres advisory lock for the span of calling `lockedFn`.
// The call is blocked until all writers end their work.
// It returns ErrLockNotAcquired if it fails to acquire the lock or if another commit is already in progress.
func (l *BranchLocker) MetadataUpdater(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, lockedFn graveler.BranchLockerFunc) (interface{}, error) {
	writerLockKey, committerLockKey := calculateBranchLockerKeys(repositoryID, branchID)
	return l.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		// try lock committer key
		var locked bool
		err := tx.GetPrimitive(&locked, `SELECT pg_try_advisory_xact_lock($1);`, committerLockKey)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", graveler.ErrLockNotAcquired, committerLockKey, err)
		}
		if !locked {
			return nil, fmt.Errorf("%w (%d)", graveler.ErrAlreadyLocked, writerLockKey)
		}
		// lock writer key
		_, err = tx.Exec(`SELECT pg_advisory_xact_lock($1);`, writerLockKey)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", graveler.ErrLockNotAcquired, writerLockKey, err)
		}
		return lockedFn()
	}, db.WithIsolationLevel(pgx.ReadCommitted))
}

func calculateBranchLockerKeys(repositoryID graveler.RepositoryID, branchID graveler.BranchID) (writerLockKey int64, committerLockKey int64) {
	h := fnv.New64()
	_, _ = h.Write([]byte(repositoryID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(branchID))
	_, _ = h.Write([]byte{0})
	writerLockKey = int64(h.Sum64())
	committerLockKey = writerLockKey + 1
	return
}
