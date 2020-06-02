package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetBranch(ctx context.Context, repo string, branch string) (*Branch, error) {
	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repo),
		"branch": ValidateBranchName(branch),
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := getRepoID(tx, repo)
		if err != nil {
			return nil, err
		}
		var b Branch
		if err := tx.Get(&b, `SELECT repository_id, id, name, next_commit FROM branches WHERE repository_id = $1 AND name = $2`, repoID, branch); err != nil {
			return nil, err
		}
		return &b, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return res.(*Branch), nil
}
