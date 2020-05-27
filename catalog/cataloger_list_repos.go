package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) ListRepos(ctx context.Context, limit int, after string) ([]*Repo, bool, error) {
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		sb := db.Builder.NewSelectBuilder()
		sb.From("repositories").
			Select("*").
			OrderBy("name").
			Where(sb.GreaterThan("name", after))
		if limit >= 0 {
			sb.Limit(limit + 1)
		}
		sql, args := sb.Build()
		var repos []*Repo
		err := tx.Select(&repos, sql, args...)
		if err != nil {
			return nil, err
		}
		c.log.WithContext(ctx).
			WithFields(logging.Fields{
				"limit": limit,
				"after": after,
			}).Debug("List repos")

		return repos, err
	}, c.transactOpts(ctx, db.ReadOnly())...)

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
