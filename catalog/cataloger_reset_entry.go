package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ResetEntry(ctx context.Context, repository, branch string, path string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
	}); err != nil {
		return err
	}
	if path == "" {
		return db.ErrNotFound
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		res, err := tx.Exec(`DELETE FROM catalog_entries WHERE branch_id=$1 AND path=$2 AND min_commit=$3`, branchID, path, MinCommitUncommitedIndicator)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, ErrEntryNotFound
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
