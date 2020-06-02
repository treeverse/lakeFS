package catalog

import "github.com/treeverse/lakefs/db"

type LockType int

const (
	LockTypeNone LockType = iota
	LockTypeShare
	LockTypeUpdate
)

func getBranchID(tx db.Tx, repo string, branch string, branchLockType LockType) (int, error) {
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
	err := tx.Get(&branchID, q, repo, branch)
	return branchID, err
}

func getRepoID(tx db.Tx, repo string) (int, error) {
	var repoID int
	err := tx.Get(&repoID, `SELECT id FROM repositories WHERE name=$1`, repo)
	return repoID, err
}
