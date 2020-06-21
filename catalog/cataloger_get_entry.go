package catalog

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetEntry(ctx context.Context, repository, reference string, path string) (*Entry, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "reference", IsValid: ValidateReference(reference)},
		{Name: "path", IsValid: ValidatePath(path)},
	}); err != nil {
		return nil, err
	}

	ref, err := ParseRef(reference)
	if err != nil {
		return nil, err
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, ref.Branch, LockTypeNone)
		if err != nil {
			return nil, err
		}

		var q string
		switch ref.CommitID {
		case CommittedID:
			q = `SELECT path, physical_address, creation_date, size, checksum, metadata
					FROM entries_lineage_committed_v
					WHERE displayed_branch = $1 AND path = $2 AND NOT is_deleted`
		case UncommittedID:
			q = `SELECT path, physical_address, creation_date, size, checksum, metadata
					FROM entries_lineage_v
					WHERE displayed_branch = $1 AND path = $2 AND NOT is_deleted`
		default:
			return nil, errors.New("TBD")
		}
		var ent Entry
		if err := tx.Get(&ent, q, branchID, path); err != nil {
			return nil, err
		}
		return &ent, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return res.(*Entry), nil
}
