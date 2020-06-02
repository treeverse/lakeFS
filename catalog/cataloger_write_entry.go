package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const (
	NoLock = iota
	ShareLock
	UpdateLock
)

func getBranchId(tx db.Tx, repo, branch string, branchLockType int) (int, error) {
	var forLock string
	switch branchLockType {
	case NoLock:
		forLock = ""
	case ShareLock:
		forLock = " FOR SHARE"
	case UpdateLock:
		forLock = " FOR UPDATE"
	}
	getBranchId := `SELECT b.id FROM branches b join repositories r 
						on r.id = b.repository_id
						WHERE r.name = $1 and b.name = $2` + forLock
	var branchID int
	err := tx.Get(&branchID, getBranchId, repo, branch) // will block merges,commits and diffs on this branch
	return branchID, err
}

func (c *cataloger) WriteEntry(ctx context.Context, repo, branch, path, checksum, physicalAddress string, size int, isStaged bool, metadata *map[string]string) error {
	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repo),
		"path":   ValidatePath(path),
		"branch": ValidateBranchName(branch),
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {

		branchID, err := getBranchId(tx, repo, branch, ShareLock) // will block merges,commits and diffs on this branch
		if err != nil {
			c.log.WithContext(ctx).
				WithError(err).
				WithFields(logging.Fields{
					"branch": branch,
					"repo":   repo,
				}).Warn("Branch not found")
			return nil, err
		}

		_, err = tx.Exec(`DELETE FROM entries WHERE branch_id = $1 AND path = $2 AND min_commit = 0`, branchID, path)
		if err != nil {
			c.log.WithContext(ctx).
				WithError(err).
				WithFields(logging.Fields{
					"branch": branch,
					"path":   path,
				}).Warn("Delete uncommitted failed")
			return nil, err
		}
		_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,metadata,size,is_staged) values ($1,$2,$3,$4,$5,$6,$7)`,
			branchID, path, physicalAddress, checksum, metadata, size, isStaged)
		if err != nil {
			c.log.WithContext(ctx).WithError(err).
				WithFields(logging.Fields{
					"branch": branch,
					"Path":   path,
				}).Warn("failed entry creation")
			return nil, err
		} else {
			return nil, nil
		}
	})
	return err
}
