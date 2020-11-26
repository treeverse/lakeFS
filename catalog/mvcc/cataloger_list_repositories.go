package mvcc

import (
	"context"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const ListRepositoriesMaxLimit = 10000

func (c *cataloger) ListRepositories(ctx context.Context, limit int, after string) ([]*catalog.Repository, bool, error) {
	if limit < 0 || limit > ListRepositoriesMaxLimit {
		limit = ListRepositoriesMaxLimit
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		query := `SELECT r.name, r.storage_namespace, b.name as default_branch, r.creation_date
			FROM catalog_repositories r JOIN catalog_branches b ON r.default_branch = b.id 
			WHERE r.name > $1
			ORDER BY r.name
			LIMIT $2`
		var repos []*catalog.Repository
		if err := tx.Select(&repos, query, after, limit+1); err != nil {
			return nil, err
		}
		return repos, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	repos := res.([]*catalog.Repository)
	hasMore := paginateSlice(&repos, limit)
	return repos, hasMore, err
}
