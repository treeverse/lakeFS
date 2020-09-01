package catalog

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/db"
)

// CreateEntries add multiple entries into the catalog, this process doesn't pass through de-dup mechanism.
//   It is mainly used by import mass entries into the catalog.
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
		// single insert per batch
		for i := 0; i < len(entries); i += c.CreateEntriesInsertSize {
			sqInsert := psql.Insert("catalog_entries").
				Columns("branch_id", "path", "physical_address", "checksum", "size", "metadata", "creation_date", "is_expired")
			j := i + c.CreateEntriesInsertSize
			if j > len(entries) {
				j = len(entries)
			}
			for _, entry := range entries[i:j] {
				creationDate := entry.CreationDate
				if entry.CreationDate.IsZero() {
					creationDate = time.Now()
				}
				sqInsert = sqInsert.Values(branchID, entry.Path, entry.PhysicalAddress, entry.Checksum, entry.Size, entry.Metadata, creationDate, entry.Expired)
			}
			query, args, err := sqInsert.Suffix(`ON CONFLICT (branch_id,path,min_commit)
DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, creation_date=EXCLUDED.creation_date, is_expired=EXCLUDED.is_expired, max_commit=catalog_max_commit_id()`).
				ToSql()
			if err != nil {
				return nil, fmt.Errorf("build query: %w", err)
			}
			_, err = tx.Exec(query, args...)
			if err != nil {
				return nil, err
			}
		}
		return nil, nil
	}, c.txOpts(ctx)...)
	return err
}
