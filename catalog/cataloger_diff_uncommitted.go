package catalog

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DiffUncommitted(ctx context.Context, repository, branch string) (Differences, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
	}); err != nil {
		return nil, err
	}
	differences, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}

		lineage, err := getLineage(tx, branchID, CommittedID)
		if err != nil {
			return nil, fmt.Errorf("get lineage: %w", err)
		}

		q := psql.Select("CASE WHEN e.max_commit=0 THEN 1 WHEN v.path IS NOT NULL THEN 2 ELSE 0 END AS diff_type", "e.path").
			FromSelect(sqEntriesV(UncommittedID), "e").
			JoinClause(
				sqEntriesLineageV(branchID, CommittedID, lineage).
					Prefix("LEFT JOIN (").Suffix(") AS v ON v.path=e.path")).
			Where(sq.Eq{"e.branch_id": branchID, "e.is_committed": false})
		sql, args, err := q.ToSql()
		if err != nil {
			return nil, fmt.Errorf("build sql: %w", err)
		}

		var result Differences
		if err := tx.Select(&result, sql, args...); err != nil {
			return nil, err
		}
		return result, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return differences.(Differences), nil
}
