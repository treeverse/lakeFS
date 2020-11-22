package mvcc

import (
	"context"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ResetEntries(ctx context.Context, repository, branch string, prefix string) error {
	if err := catalog.Validate(catalog.ValidateFields{
		{Name: "repository", IsValid: catalog.ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: catalog.ValidateBranchName(branch)},
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		prefixCond := db.Prefix(prefix)
		_, err = tx.Exec(`DELETE FROM catalog_entries WHERE branch_id=$1 AND path LIKE $2 AND min_commit=$3`, branchID, prefixCond, catalog.MinCommitUncommittedIndicator)
		return nil, err
	}, c.txOpts(ctx)...)
	return err
}
