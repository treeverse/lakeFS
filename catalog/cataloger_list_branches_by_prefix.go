package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ListBranchesByPrefix(ctx context.Context, repo string, prefix string, limit int, after string) ([]*Branch, bool, error) {
	if err := Validate(ValidateFields{
		"repo": ValidateRepoName(repo),
	}); err != nil {
		return nil, false, err
	}

	prefixCond := db.Prefix(prefix)
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := getRepoIDByName(tx, repo)
		if err != nil {
			return nil, err
		}

		query := `SELECT repository_id, id, name, next_commit
			FROM branches
			WHERE repository_id = $1 AND name like $2 AND name > $3
			ORDER BY name`
		args := []interface{}{repoID, prefixCond, after}
		if limit >= 0 {
			query += ` LIMIT $4`
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
	// has more support - we read extra one and it is the indicator for more
	hasMore := false
	if limit >= 0 && len(branches) > limit {
		branches = branches[0:limit]
		hasMore = true
	}
	return branches, hasMore, err
}
