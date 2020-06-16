package catalog

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetEntry(ctx context.Context, repository, branch string, commitID CommitID, path string) (*Entry, error) {
	if err := Validate(ValidateFields{
		"repository": ValidateRepositoryName(repository),
		"branch":     ValidateBranchName(branch),
		"path":       ValidatePath(path),
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeNone)
		if err != nil {
			return nil, err
		}

		var q string
		switch commitID {
		case CommittedID:
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, metadata, min_commit, max_commit, is_tombstone
					FROM entries_lineage_committed_v
					WHERE displayed_branch = $1 AND path = $2 AND NOT is_deleted`
		case UncommittedID:
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, metadata, min_commit, max_commit, is_tombstone
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
