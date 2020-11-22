package mvcc

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/catalog"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) DeleteBranch(ctx context.Context, repository, branch string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeUpdate)
		if err != nil {
			return nil, err
		}

		// default branch doesn't have parents
		var legacyCount int
		err = tx.GetPrimitive(&legacyCount, `SELECT array_length(lineage,1) FROM catalog_branches WHERE id=$1`, branchID)
		if err != nil {
			return nil, err
		}
		if legacyCount == 0 {
			return nil, fmt.Errorf("delete default branch: %w", catalog.ErrOperationNotPermitted)
		}

		// check we don't have branch depends on us by count lineage records we are part of
		var childBranches int
		err = tx.GetPrimitive(&childBranches, `SELECT count(*) FROM catalog_branches b 
			JOIN catalog_branches b2 ON b.repository_id = b2.repository_id AND b2.id=$1
			WHERE $1=ANY(b.lineage)`, branchID)
		if err != nil {
			return nil, fmt.Errorf("dependent check: %w", err)
		}
		if childBranches > 0 {
			return nil, fmt.Errorf("branch has dependent branch: %w", catalog.ErrOperationNotPermitted)
		}

		// delete branch entries
		_, err = tx.Exec(`DELETE FROM catalog_entries WHERE branch_id=$1`, branchID)
		if err != nil {
			return nil, fmt.Errorf("delete entries: %w", err)
		}

		// delete branch
		res, err := tx.Exec(`DELETE FROM catalog_branches WHERE id=$1`, branchID)
		if err != nil {
			return nil, fmt.Errorf("delete branch: %w", err)
		}
		affected := res.RowsAffected()
		if affected != 1 {
			return nil, catalog.ErrBranchNotFound
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
