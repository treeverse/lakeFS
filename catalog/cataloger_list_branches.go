package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*Branch, bool, error) {
	if err := Validate(ValidateFields{
		"repository": ValidateRepositoryName(repository),
	}); err != nil {
		return nil, false, err
	}

	prefixCond := db.Prefix(prefix)
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := getRepositoryID(tx, repository)
		if err != nil {
			return nil, err
		}

		query := `SELECT $2 AS repository, name
			FROM branches
			WHERE repository_id = $1 AND name like $3 AND name > $4
			ORDER BY name`
		args := []interface{}{repoID, repository, prefixCond, after}
		if limit >= 0 {
			query += ` LIMIT $5`
			args = append(args, limit+1)
		}

		var branches []*Branch
		if err := tx.Select(&branches, query, args...); err != nil {
			return nil, err
		}
		return branches, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	branches := res.([]*Branch)
	hasMore := paginateSlice(&branches, limit)
	return branches, hasMore, err
}
