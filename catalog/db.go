package catalog

import (
	"reflect"

	"github.com/treeverse/lakefs/db"
)

type LockType int

const (
	LockTypeNone LockType = iota
	LockTypeShare
	LockTypeUpdate
)

func getBranchID(tx db.Tx, repository, branch string, branchLockType LockType) (int, error) {
	const b = `SELECT b.id FROM branches b join repositories r 
					ON r.id = b.repository_id
					WHERE r.name = $1 AND b.name = $2`
	var q string
	switch branchLockType {
	case LockTypeNone:
		q = b
	case LockTypeShare:
		q = b + " FOR SHARE"
	case LockTypeUpdate:
		q = b + " FOR UPDATE"
	default:
		return 0, ErrInvalidLockValue
	}
	// will block merges, commits and diffs on this branch
	var branchID int
	err := tx.Get(&branchID, q, repository, branch)
	return branchID, err
}

func getRepoID(tx db.Tx, repository string) (int, error) {
	var repoID int
	err := tx.Get(&repoID, `SELECT id FROM repositories WHERE name=$1`, repository)
	return repoID, err
}

func getCommitID(tx db.Tx, branchID int) (int, error) {
	var commitID int
	err := tx.Get(&commitID, `SELECT next_commit FROM branches WHERE id = $1`, branchID)
	return commitID, err
}

// paginateSlice take slice address, resize and return 'has more' when needed
func paginateSlice(s interface{}, limit int) bool {
	if limit <= 0 {
		return false
	}
	v := reflect.ValueOf(s)
	if v.Kind() != reflect.Ptr {
		return false
	}
	el := v.Elem()
	if el.Len() > limit {
		el.Set(el.Slice(0, limit))
		return true
	}
	return false
}
