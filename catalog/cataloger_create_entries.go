package catalog

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateEntries(ctx context.Context, repository, branch string, entries []Entry) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
	}); err != nil {
		return err
	}
	// more validation
	for i := range entries {
		if !IsNonEmptyString(entries[i].Path) {
			return fmt.Errorf("entry at pos %d, path: %w", i, ErrInvalidValue)
		}
	}
	if len(entries) == 0 {
		return nil
	}
	// create entries
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		for i := range entries {
			if _, err := insertNewEntry(tx, branchID, &entries[i]); err != nil {
				return nil, fmt.Errorf("entry at pos %d: %w", i, err)
			}
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
