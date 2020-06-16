package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteRepository(ctx context.Context, repository string) error {
	if err := Validate(ValidateFields{
		"repository": ValidateRepositoryName(repository),
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		res, err := tx.Exec(`DELETE FROM repositories WHERE name=$1`, repository)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, ErrRepoNotFound
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
