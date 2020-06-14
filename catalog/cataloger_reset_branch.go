package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ResetBranch(ctx context.Context, repository, branch string) error {
	if err := Validate(ValidateFields{
		"repository": ValidateRepositoryName(repository),
		"branch":     ValidateBranchName(branch),
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		res, err := tx.Exec(`DELETE FROM entries WHERE branch_id = $1 AND min_commit = 0`, branchID)
		if err != nil {
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		return affected, nil
	}, c.txOpts(ctx)...)
	return err
}
