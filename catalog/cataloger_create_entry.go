package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) CreateEntry(ctx context.Context, repository, branch string, entry Entry) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "path", IsValid: ValidatePath(entry.Path)},
	}); err != nil {
		return err
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		_, err = insertNewEntry(tx, branchID, &entry)
		return nil, err
	}, c.txOpts(ctx)...)
	return err
}

func insertNewEntry(tx db.Tx, branchID int64, entry *Entry) (string, error) {
	var ctid string
	err := tx.Get(&ctid, `INSERT INTO entries (branch_id,path,physical_address,checksum,size,metadata) VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (branch_id,path,min_commit)
			DO UPDATE SET physical_address=$3, checksum=$4, size=$5, metadata=$6, max_commit=$7
			RETURNING ctid`,
		branchID, entry.Path, entry.PhysicalAddress, entry.Checksum, entry.Size, entry.Metadata, MaxCommitID)
	return ctid, err
}
