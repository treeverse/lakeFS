package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateEntryDedup(ctx context.Context, repository, branch string, entry Entry, dedup DedupParams) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "path", IsValid: ValidatePath(entry.Path)},
	}); err != nil {
		return err
	}
	res, err := c.runDBJob(dbJobFunc(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		return insertNewEntry(tx, branchID, &entry)
	}))
	if err != nil {
		return err
	}

	// post request to dedup
	if dedup.ID != "" {
		c.dedupCh <- &dedupRequest{
			Repository:       repository,
			StorageNamespace: dedup.StorageNamespace,
			DedupID:          dedup.ID,
			Entry:            &entry,
			EntryCTID:        res.(string),
			DedupResultCh:    dedup.Ch,
		}
	}
	return nil
}
