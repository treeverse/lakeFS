package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetBranchReference(ctx context.Context, repository, branch string) (string, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
	}); err != nil {
		return "", err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return "", err
		}

		var commitID CommitID
		err = tx.GetPrimitive(&commitID, `SELECT COALESCE(MAX(commit_id),0) as commit_id FROM catalog_commits WHERE branch_id=$1`, branchID)
		if err != nil {
			return "", err
		}
		if commitID == 0 {
			return "", ErrCommitNotFound
		}
		return MakeReference(branch, commitID), nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}
