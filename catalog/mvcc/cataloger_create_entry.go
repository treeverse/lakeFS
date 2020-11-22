package mvcc

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateEntry(ctx context.Context, repository, branch string, entry catalog.Entry, params catalog.CreateEntryParams) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "path", IsValid: ValidatePath(entry.Path)},
	}); err != nil {
		return err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := c.getBranchIDCache(tx, repository, branch)
		if err != nil {
			return nil, err
		}
		return insertEntry(tx, branchID, &entry)
	}, c.txOpts(ctx)...)
	if err != nil {
		return err
	}

	// post request to dedup if needed
	if params.Dedup.ID != "" {
		c.dedupCh <- &dedupRequest{
			Repository:       repository,
			StorageNamespace: params.Dedup.StorageNamespace,
			DedupID:          params.Dedup.ID,
			Entry:            &entry,
			EntryCTID:        res.(string),
		}
	}
	return nil
}

func insertEntry(tx db.Tx, branchID int64, entry *catalog.Entry) (string, error) {
	var (
		ctid   string
		dbTime sql.NullTime
	)
	if entry.CreationDate.IsZero() {
		dbTime.Valid = false
	} else {
		dbTime.Time = entry.CreationDate
		dbTime.Valid = true
	}
	err := tx.GetPrimitive(&ctid, `INSERT INTO catalog_entries (branch_id,path,physical_address,checksum,size,metadata,creation_date,is_expired,min_commit)
                        VALUES ($1,$2,$3,$4,$5,$6,COALESCE($7,NOW()),$8,$9)
			ON CONFLICT (branch_id,path,min_commit)
			DO UPDATE SET physical_address=EXCLUDED.physical_address, checksum=EXCLUDED.checksum, size=EXCLUDED.size, metadata=EXCLUDED.metadata, creation_date=EXCLUDED.creation_date, is_expired=EXCLUDED.is_expired, min_commit=EXCLUDED.min_commit, max_commit=$9
			RETURNING ctid`,
		branchID, entry.Path, entry.PhysicalAddress, entry.Checksum, entry.Size, entry.Metadata, dbTime, entry.Expired, MaxCommitID)
	if err != nil {
		return "", fmt.Errorf("insert entry: %w", err)
	}
	return ctid, nil
}
