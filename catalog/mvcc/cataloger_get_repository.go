package mvcc

import (
	"context"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetRepository(ctx context.Context, repository string) (*catalog.Repository, error) {
	if err := catalog.Validate(catalog.ValidateFields{
		{Name: "repository", IsValid: catalog.ValidateRepositoryName(repository)},
	}); err != nil {
		return nil, err
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		return c.getRepositoryCache(tx, repository)
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*catalog.Repository), nil
}
