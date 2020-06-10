package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) ListRepositories(ctx context.Context, limit int, after string) ([]*Repo, bool, error) {
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		query := `SELECT r.name, r.storage_namespace, b.name as default_branch, r.creation_date
			FROM repositories r JOIN branches b ON r.default_branch = b.id 
			WHERE r.name > $1
			ORDER BY r.name`
		args := []interface{}{after}
		if limit >= 0 {
			query += ` LIMIT $2`
			args = append(args, limit+1)
		}

		var repos []*Repo
		if err := tx.Select(&repos, query, args...); err != nil {
			return nil, err
		}
		c.log.WithContext(ctx).
			WithFields(logging.Fields{
				"limit": limit,
				"after": after,
			}).Debug("List repos")

		return repos, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	repos := res.([]*Repo)
	// has more support - we read extra one and it is the indicator for more
	hasMore := false
	if limit >= 0 && len(repos) > limit {
		repos = repos[0:limit]
		hasMore = true
	}
	return repos, hasMore, err
}
