package mvcc

import (
	"context"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const ListBranchesMaxLimit = 10000

func (c *cataloger) ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*catalog.Branch, bool, error) {
	if err := catalog.Validate(catalog.ValidateFields{
		{Name: "repository", IsValid: catalog.ValidateRepositoryName(repository)},
	}); err != nil {
		return nil, false, err
	}
	if limit < 0 || limit > ListBranchesMaxLimit {
		limit = ListBranchesMaxLimit
	}
	prefixCond := db.Prefix(prefix)
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := c.getRepositoryIDCache(tx, repository)
		if err != nil {
			return nil, err
		}

		query := `SELECT $2 AS repository, name
			FROM catalog_branches
			WHERE repository_id = $1 AND name like $3 AND name > $4
			ORDER BY name
			LIMIT $5`
		var branches []*catalog.Branch
		if err := tx.Select(&branches, query, repoID, repository, prefixCond, after, limit+1); err != nil {
			return nil, err
		}
		return branches, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	branches := res.([]*catalog.Branch)
	hasMore := paginateSlice(&branches, limit)
	return branches, hasMore, nil
}
