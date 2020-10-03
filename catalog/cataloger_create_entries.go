package catalog

import (
	"context"
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
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

	// validate that we have path on each entry and remember last entry based on path (for dup remove)
	entriesMap := make(map[string]*Entry, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		p := entries[i].Path
		if !IsNonEmptyString(p) {
			return fmt.Errorf("entry at pos %d, path: %w", i, ErrInvalidValue)
		}
		entriesMap[p] = &entries[i]
	}

	// prepare a list of entries to insert without duplicates
	entriesToInsert := make([]*Entry, 0, len(entriesMap))
	for i := range entries {
		ent := entriesMap[entries[i].Path]
		if &entries[i] == ent {
			entriesToInsert = append(entriesToInsert, ent)
		}
	}

	// create entries
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		// single insert per batch
		entriesInsertSize := c.BatchWrite.EntriesInsertSize
		for i := 0; i < len(entriesToInsert); i += entriesInsertSize {
			sqInsert := psql.Insert("catalog_entries").
				Columns("branch_id", "path", "physical_address", "checksum", "size", "metadata", "creation_date", "is_expired")
			j := i + entriesInsertSize
			if j > len(entriesToInsert) {
				j = len(entriesToInsert)
			}
			for _, entry := range entriesToInsert[i:j] {
				var dbTime sql.NullTime
				if !entry.CreationDate.IsZero() {
					dbTime.Time = entry.CreationDate
					dbTime.Valid = true
				}
				sqInsert = sqInsert.Values(branchID, entry.Path, entry.PhysicalAddress, entry.Checksum, entry.Size, entry.Metadata,
					sq.Expr("COALESCE(?,NOW())", dbTime), entry.Expired)
			}
			query, args, err := sqInsert.Suffix(`ON CONFLICT (branch_id,path,min_commit)
DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, creation_date=EXCLUDED.creation_date, is_expired=EXCLUDED.is_expired, max_commit=?`, MaxCommitID).
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
