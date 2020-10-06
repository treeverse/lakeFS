package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ResetBranch(ctx context.Context, repository, branch string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		res, err := tx.Exec(`DELETE FROM catalog_entries WHERE branch_id=$1 AND min_commit=$2`, branchID, MinCommitUncommittedIndicator)
		if err != nil {
			return nil, err
		}
		_, err = res.RowsAffected()
		return nil, err
	}, c.txOpts(ctx)...)
	return err
}
