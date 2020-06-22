package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateBranch(ctx context.Context, repository, branch string, sourceBranch string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "sourceBranch", IsValid: ValidateBranchName(sourceBranch)},
	}); err != nil {
		return err
	}

	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := getRepositoryID(tx, repository)
		if err != nil {
			return nil, err
		}

		// get source branch id
		var sourceBranchID int
		if err := tx.Get(&sourceBranchID, `SELECT id FROM branches WHERE repository_id = $1 AND name = $2`, repoID, sourceBranch); err != nil {
			return nil, err
		}

		// next id for branch
		var branchID int
		if err := tx.Get(&branchID, `SELECT nextval('branches_id_seq');`); err != nil {
			return nil, err
		}

		// insert new branch
		if _, err := tx.Exec(`INSERT INTO branches (repository_id, id, name) VALUES($1, $2, $3)`, repoID, branchID, branch); err != nil {
			return nil, err
		}

		// insert new lineage for the new branch
		_, err = tx.Exec(`INSERT INTO lineage (branch_id, precedence, ancestor_branch, effective_commit)
			SELECT $1, precedence + 1, ancestor_branch, effective_commit
			FROM lineage_v
			WHERE branch_id = $2 AND active_lineage`, branchID, sourceBranchID)
		if err != nil {
			return nil, err
		}
		return branchID, nil
	}, c.txOpts(ctx)...)
	return err
}
