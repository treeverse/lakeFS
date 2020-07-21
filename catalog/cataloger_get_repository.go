package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetRepository(ctx context.Context, repository string) (*Repository, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
	}); err != nil {
		return nil, err
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		return c.getRepositoryCache(tx, repository)
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*Repository), nil
}
