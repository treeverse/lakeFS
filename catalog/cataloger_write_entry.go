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
	getBranchId := `SELECT branch_id FROM branches b join repositories r 
						on r.id = b.repository_id
						WHERE r.name = $1 and b.name = $2` + forLock
	var branchId int
	err := tx.Get(&branchId, getBranchId, repo, branch) // will block merges,commits and diffs on this branch
	return branchId, err
}

func (c *cataloger) WriteEntry(ctx context.Context, repoName, branchName, path, checksum, physicalAddress string, size int, isStaged bool, metadata map[string]string) error {

	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repoName),
		"path":   ValidatePath(path),
		"branch": ValidateBranchName(branchName),
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {

		branchId, err := getBranchId(tx, repoName, branchName, ShareLock) // will block merges,commits and diffs on this branch
		if err != nil {
			c.log.WithContext(ctx).
				WithError(err).
				WithFields(logging.Fields{
					"branch": branchName,
					"repo":   repoName,
				}).Warn("Branch not found")
			return nil, err
		}

		_, err = tx.Exec(`DELETE FROM entries WHERE branch_id = $1 AND path = $2 AND min_commit = 0`, branchId, path)
		if err != nil {
			c.log.WithContext(ctx).
				WithError(err).
				WithFields(logging.Fields{
					"branch": branchName,
					"path":   path,
				}).Warn("Delete uncommitted failed")
			return nil, err
		}
		_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,metadata,size,is_staged) values ($1,$2,$3,$4,$5,$6,$7)`,
			branchId, path, physicalAddress, checksum, metadata, size, isStaged)
		if err != nil {
			c.log.WithContext(ctx).WithError(err).
				WithFields(logging.Fields{
					"branch": branchName,
					"Path":   path,
				}).Warn("failed entry creation")
			return nil, err
		} else {
			return nil, nil
		}
	})
	return err
}
