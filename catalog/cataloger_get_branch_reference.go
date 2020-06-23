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
		nextCommitID, err := getNextCommitID(tx, branchID)
		if err != nil {
			return "", err
		}
		commitID := nextCommitID - 1
		if commitID <= 0 {
			return "", nil
		}
		reference := MakeReference(branch, commitID)
		return reference, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return "", err
	}
	return res.(string), nil
}
