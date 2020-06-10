package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
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
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}

		_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata) VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (branch_id,path,min_commit)
			DO UPDATE SET physical_address=$3, checksum=$4, size=$5, metadata=$6, max_commit=$7`,
			branchID, path, physicalAddress, checksum, size, metadata, MaxCommitID)
		return nil, err
	}, c.txOpts(ctx)...)
	return err
}
