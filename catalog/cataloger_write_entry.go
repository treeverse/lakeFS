package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) WriteEntry(ctx context.Context, repo, branch, path, checksum, physicalAddress string, size int, metadata *map[string]string) error {
	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repo),
		"path":   ValidatePath(path),
		"branch": ValidateBranchName(branch),
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repo, branch, LockTypeShare) // will block merges,commits and diffs on this branch
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
		_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,metadata,size) values ($1,$2,$3,$4,$5,$6)`,
			branchID, path, physicalAddress, checksum, metadata, size)
		if err != nil {
			c.log.WithContext(ctx).WithError(err).
				WithFields(logging.Fields{
					"branch": branch,
					"path":   path,
				}).Warn("failed entry creation")
			return nil, err
		} else {
			return nil, nil
		}
	})
	return err
}
