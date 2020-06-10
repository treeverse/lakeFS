package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetRepository(ctx context.Context, repository string) (*Repo, error) {
	if err := Validate(ValidateFields{
		"repository": ValidateRepoName(repository),
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		var r Repo
		err := tx.Get(&r, `SELECT r.name, r.storage_namespace, b.name as default_branch, r.creation_date
 			FROM repositories r, branches b
			WHERE r.id = b.repository_id AND r.default_branch = b.id AND r.name = $1`,
			repository)
		if err != nil {
			return nil, err
		}
		return &r, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*Repo), nil
}
