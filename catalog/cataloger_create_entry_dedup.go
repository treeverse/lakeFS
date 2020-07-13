package catalog

import (
	"context"
	"database/sql"
	"fmt"

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
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		_, err = insertEntry(tx, branchID, &entry)
		return nil, err
	}, c.txOpts(ctx)...)
	if err != nil {
		return err
	}

	// post request to dedup if needed
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

func insertEntry(tx db.Tx, branchID int64, entry *Entry) (string, error) {
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
	err := tx.Get(&ctid, `INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata,creation_date)
                        VALUES ($1,$2,$3,$4,$5,$6, COALESCE($7, NOW()))
			ON CONFLICT (branch_id,path,min_commit)
			DO UPDATE SET physical_address=$3, checksum=$4, size=$5, metadata=$6, creation_date=EXCLUDED.creation_date, max_commit=$8
			RETURNING ctid`,
		branchID, entry.Path, entry.PhysicalAddress, entry.Checksum, entry.Size, entry.Metadata, dbTime, MaxCommitID)
	if err != nil {
		return "", fmt.Errorf("insert entry: %w", err)
	}
	return ctid, nil
}
