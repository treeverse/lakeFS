package catalog

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) SetDefaultBranch(ctx context.Context, repository, branch string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := c.getRepositoryIDCache(tx, repository)
		if err != nil {
			return nil, err
		}

		var branchID int64
		if err := tx.Get(&branchID, `SELECT id FROM catalog_branches WHERE repository_id=$1 AND name=$2`,
			repoID, branch); err != nil {
			return nil, fmt.Errorf("source branch id: %w", err)
		}

		_, err = tx.Exec(`UPDATE catalog_repositories SET default_branch=$2 WHERE id=$1`, repoID, branchID)
		if err != nil {
			return nil, fmt.Errorf("update default branch: %w", err)
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
