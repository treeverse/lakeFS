package catalog

import (
	"fmt"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ListRepos(opts ...func(*ListReposOptions)) ([]*Repo, bool, error) {
	options := newListReposOptions(opts)
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		sb := db.Builder.
			NewSelectBuilder().
			From("repositories").
			Select("*")
		switch options.Field {
		case ListRepoFieldID, ListRepoFieldName:
			sb.Where(sb.GreaterThan(options.Field, options.After))
			sb.OrderBy(options.Field)
		default:
			return nil, fmt.Errorf("unsupported field: %s", options.Field)
		}
		if options.Limit >= 0 {
			sb.Limit(options.Limit + 1)
		}
		sql, args := sb.Build()
		var repos []*Repo
		err := tx.Select(&repos, sql, args...)
		return repos, err
	}, c.transactOpts(db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	repos := res.([]*Repo)
	// has more support - we read extra one and it is the indicator for more
	hasMore := false
	if options.Limit >= 0 && len(repos) > options.Limit {
		repos = repos[0:options.Limit]
		hasMore = true
	}
	return repos, hasMore, err
}

func newListReposOptions(opts []func(*ListReposOptions)) *ListReposOptions {
	options := &ListReposOptions{
		Field: ListRepoDefaultField,
		Limit: ListRepoDefaultLimit,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}
