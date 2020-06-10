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

		_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata) VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (branch_id,path,min_commit)
			DO UPDATE SET physical_address=$3, checksum=$4, size=$5, metadata=$6, max_commit=('01111111111111111111111111111111'::"bit")::integer`,
			branchID, path, physicalAddress, checksum, size, metadata)
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
