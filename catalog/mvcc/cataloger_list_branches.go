package mvcc

import (
	"context"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const ListBranchesMaxLimit = 10000

func (c *cataloger) ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*catalog.Branch, bool, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
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

		const query = `SELECT b.name, (SELECT MAX(commit_id) FROM catalog_commits WHERE catalog_commits.branch_id = b.id) AS commit_id
			FROM catalog_branches b
			WHERE b.repository_id = $1 AND b.name like $2 AND b.name > $3
			ORDER BY b.name
			LIMIT $4`
		var branchesNameAndCommit []struct {
			Name     string `db:"name"`
			CommitID int64  `db:"commit_id"`
		}
		if err := tx.Select(&branchesNameAndCommit, query, repoID, prefixCond, after, limit+1); err != nil {
			return nil, err
		}
		branches := make([]*catalog.Branch, len(branchesNameAndCommit))
		for i, nc := range branchesNameAndCommit {
			branches[i] = &catalog.Branch{
				Name:      nc.Name,
				Reference: MakeReference(nc.Name, CommitID(nc.CommitID)),
			}
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
