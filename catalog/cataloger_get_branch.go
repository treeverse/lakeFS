package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetBranch(ctx context.Context, repository, branch string) (*Branch, error) {
	if err := Validate(ValidateFields{
		"repository": ValidateRepositoryName(repository),
		"branch":     ValidateBranchName(branch),
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := getRepositoryID(tx, repository)
		if err != nil {
			return nil, err
		}
		var b Branch
		if err := tx.Get(&b, `SELECT r.name as repository, b.name
			FROM repositories r JOIN branches b ON r.id = b.repository_id
			WHERE r.id=$1 AND b.name=$2`, repoID, branch); err != nil {
			return nil, err
		}
		return &b, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return res.(*Branch), nil
}
