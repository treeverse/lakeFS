package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DiffUncommitted(ctx context.Context, repository, branch string) (Differences, error) {
	if err := Validate(ValidateFields{
		"repository": ValidateRepositoryName(repository),
		"branch":     ValidateBranchName(branch),
	}); err != nil {
		return nil, err
	}
	differences, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeNone)
		if err != nil {
			return nil, err
		}
		const diffSQL = `
SELECT CASE
           WHEN DifferenceTypeRemoved THEN 1
           WHEN DifferenceTypeChanged THEN 2
           ELSE 0
           END AS diff_type,
       path
FROM (
         SELECT u.path,
                u.max_commit = 0   AS DifferenceTypeRemoved,
                c.path IS NOT NULL AS DifferenceTypeChanged
         FROM entries u
                  LEFT JOIN entries_lineage_committed_v c
                            ON c.path = u.path AND c.displayed_branch = u.branch_id
         WHERE u.branch_id = $1
           AND u.min_commit = 0
     ) as t;`
		var result Differences
		if err := tx.Select(&result, diffSQL, branchID); err != nil {
			return nil, err
		}
		return result, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return differences.(Differences), nil
}
