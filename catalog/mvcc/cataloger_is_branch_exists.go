package mvcc

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/catalog"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) BranchExists(ctx context.Context, repository, branch string) (bool, error) {
	if err := catalog.Validate(catalog.ValidateFields{
		{Name: "repository", IsValid: catalog.ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: catalog.ValidateBranchName(branch)},
	}); err != nil {
		return false, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		_, err := c.getBranchIDCache(tx, repository, branch)
		if errors.Is(err, db.ErrNotFound) {
			return false, nil
		}
		if err != nil {
			return nil, err
		}
		return true, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return false, err
	}
	return res.(bool), nil
}
