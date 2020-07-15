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
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.cache.BranchID(repository, branch, func(repository string, branch string) (int64, error) {
			return getBranchID(tx, repository, branch, LockTypeNone)
		})
		if err != nil {
			return nil, err
		}
		return insertNewEntry(tx, branchID, &entry)
	}, c.txOpts(ctx)...)

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
