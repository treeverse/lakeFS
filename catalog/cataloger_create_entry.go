package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) CreateEntry(ctx context.Context, repository string, branch string, path string, checksum string, physicalAddress string, size int, metadata Metadata) error {
	if err := Validate(ValidateFields{
		"repository": ValidateRepoName(repository),
		"branch":     ValidateBranchName(branch),
		"path":       ValidatePath(path),
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare) // will block merges,commits and diffs on this branch
		if err != nil {
			c.log.WithContext(ctx).
				WithError(err).
				WithFields(logging.Fields{
					"branch":     branch,
					"repository": repository,
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
		// TODO(barak): upsert and remove the above DELETE
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
	}, c.txOpts(ctx)...)
	return err
}
