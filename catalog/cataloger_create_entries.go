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

	// nothing to do in case we don't have entries
	if len(entries) == 0 {
		return nil
	}

	// validate that we have path on each entry
	for i := range entries {
		if !IsNonEmptyString(entries[i].Path) {
			return fmt.Errorf("entry at pos %d, path: %w", i, ErrInvalidValue)
		}
	}

	// create entries
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		for i := range entries {
			if _, err := insertEntry(tx, branchID, &entries[i]); err != nil {
				return nil, fmt.Errorf("entry at %d: %w", i, err)
			}
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
