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
		branchID, err := getBranchID(tx, repository, branch, LockTypeNone)
		if err != nil {
			return "", err
		}

		var commitID CommitID
		err = tx.Get(&commitID, `SELECT COALESCE(MAX(commit_id),0) as commit_id FROM commits WHERE branch_id=$1`, branchID)
		if err != nil {
			return "", err
		}
		if commitID == 0 {
			// TODO(barak): recursive search for parent commit - if none, return ErrReferenceNotFound
			return "", nil
		}
		return MakeReference(branch, commitID), nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}
