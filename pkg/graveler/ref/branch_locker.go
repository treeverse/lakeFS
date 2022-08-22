package ref

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/jackc/pgx/v4"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
)

// BranchLocker enforces the branch locking logic with Postgres advisory lock
// The lock can be held by an arbitrary number of Writers or a single MetadataUpdater.
type BranchLocker struct {
	db db.Database
}

func NewBranchLocker(db db.Database) *BranchLocker {
	return &BranchLocker{
		db: db,
	}
}

// Writer tries to acquire write lock using a Postgres advisory lock for the span of calling `lockedFn`.
// Returns ErrLockNotAcquired if it cannot acquire the lock.
func (l *BranchLocker) Writer(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, lockedFn graveler.BranchLockerFunc) (interface{}, error) {
	writerLockKey := calculateBranchLockerKey(repository.RepositoryID, branchID)
	return l.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		// try to get a shared lock on the writer key
		_, err := tx.Exec(`SELECT pg_advisory_xact_lock_shared($1);`, writerLockKey)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", graveler.ErrLockNotAcquired, writerLockKey, err)
		}
		return lockedFn()
	}, db.WithIsolationLevel(pgx.ReadCommitted))
}

// MetadataUpdater tries to lock as committer using a Postgres advisory lock for the span of calling `lockedFn`.
// It returns ErrLockNotAcquired if it fails to acquire the lock.
func (l *BranchLocker) MetadataUpdater(ctx context.Context, repository *graveler.RepositoryRecord, branchID graveler.BranchID, lockedFn graveler.BranchLockerFunc) (interface{}, error) {
	writerLockKey := calculateBranchLockerKey(repository.RepositoryID, branchID)
	return l.db.Transact(ctx, func(tx db.Tx) (interface{}, error) {
		_, err := tx.Exec(`SELECT pg_advisory_xact_lock($1);`, writerLockKey)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", graveler.ErrLockNotAcquired, writerLockKey, err)
		}
		return lockedFn()
	}, db.WithIsolationLevel(pgx.ReadCommitted))
}

func calculateBranchLockerKey(repositoryID graveler.RepositoryID, branchID graveler.BranchID) int64 {
	h := fnv.New64()
	_, _ = h.Write([]byte(repositoryID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(branchID))
	_, _ = h.Write([]byte{0})
	return int64(h.Sum64())
}
