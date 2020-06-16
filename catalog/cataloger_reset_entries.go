package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ResetEntries(ctx context.Context, repository, branch string, prefix string) error {
	if err := Validate(ValidateFields{
		"repository": ValidateRepositoryName(repository),
		"branch":     ValidateBranchName(branch),
		"prefix":     ValidatePath(prefix),
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		prefixCond := db.Prefix(prefix)
		_, err = tx.Exec(`DELETE FROM entries WHERE branch_id = $1 AND path LIKE $2 AND min_commit = 0`, branchID, prefixCond)
		return nil, err
	}, c.txOpts(ctx)...)
	return err
}
