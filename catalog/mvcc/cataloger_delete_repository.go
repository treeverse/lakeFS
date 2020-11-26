package mvcc

import (
	"context"

	"github.com/treeverse/lakefs/catalog"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteRepository(ctx context.Context, repository string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		res, err := tx.Exec(`DELETE FROM catalog_repositories WHERE name=$1`, repository)
		if err != nil {
			return nil, err
		}
		affected := res.RowsAffected()
		if affected != 1 {
			return nil, catalog.ErrRepositoryNotFound
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
