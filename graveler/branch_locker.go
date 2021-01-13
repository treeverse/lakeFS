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
// allow concurrent writers to branch
// metadata update commands will wait until all current running writers are done
// while metadata update is running or waiting to run, writes and metadata update commands will be blocked
type BranchLocker struct {
	db db.Database
}

func NewBranchLocker(db db.Database) *BranchLocker {
	return &BranchLocker{
		db: db,
	}
}

// Writer try to lock as writer, using postgres advisory lock for the span of calling `lockedCB`.
// returns ErrLockNotAcquired in case we fail to acquire the lock or commit is in progress
func (l *BranchLocker) Writer(ctx context.Context, repositoryID RepositoryID, branchID BranchID, lockedCB func() (interface{}, error)) (interface{}, error) {
	key1, _ := calculateBranchLockerKeys(repositoryID, branchID)
	return l.db.Transact(func(tx db.Tx) (interface{}, error) {
		// try lock committer key
		var locked bool
		err := tx.GetPrimitive(&locked, `SELECT pg_try_advisory_xact_lock_shared($1)`, key1)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", ErrLockNotAcquired, key1, err)
		}
		if !locked {
			return nil, fmt.Errorf("%w (%d)", ErrLockNotAcquired, key1)
		}
		return lockedCB()
	}, db.WithContext(ctx), db.WithIsolationLevel(pgx.ReadCommitted))
}

// MetadataUpdater try to lock as committer, using postgres advisory lock for the span of calling `lockedCB`.
// The call is blocked if all until all writers ends their work before calling the callback.
// returns ErrLockNotAcquired in case we fail to acquire the lock or committer already acquired the same lock
func (l *BranchLocker) MetadataUpdater(ctx context.Context, repositoryID RepositoryID, branchID BranchID, lockedCB func() (interface{}, error)) (interface{}, error) {
	key1, key2 := calculateBranchLockerKeys(repositoryID, branchID)
	return l.db.Transact(func(tx db.Tx) (interface{}, error) {
		// try lock committer key
		var locked bool
		err := tx.GetPrimitive(&locked, `SELECT pg_try_advisory_xact_lock($1);`, key2)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", ErrLockNotAcquired, key2, err)
		}
		if !locked {
			return nil, fmt.Errorf("%w (%d)", ErrLockNotAcquired, key1)
		}
		// lock writer key
		_, err = tx.Exec(`SELECT pg_advisory_xact_lock($1);`, key1)
		if err != nil {
			return nil, fmt.Errorf("%w (%d): %s", ErrLockNotAcquired, key1, err)
		}
		if !locked {
			return nil, fmt.Errorf("%w (%d)", ErrLockNotAcquired, key1)
		}
		return lockedCB()
	}, db.WithContext(ctx), db.WithIsolationLevel(pgx.ReadCommitted))
}

func calculateBranchLockerKeys(repositoryID RepositoryID, branchID BranchID) (int32, int32) {
	h := fnv.New32()
	_, _ = h.Write([]byte(repositoryID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(branchID))
	_, _ = h.Write([]byte{0})
	key1 := int32(h.Sum32())
	key2 := key1 + 1
	return key1, key2
}
