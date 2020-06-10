package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) GetEntry(ctx context.Context, repository string, branch string, path string, readUncommitted bool) (*Entry, error) {
	if err := Validate(ValidateFields{
		"repository": ValidateRepoName(repository),
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
		if readUncommitted {
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, metadata, min_commit, max_commit, is_tombstone
					FROM entries_lineage_v
					WHERE displayed_branch = $1 AND path = $2 AND NOT is_deleted`
		} else {
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, metadata, min_commit, max_commit, is_tombstone
					FROM entries_lineage_committed_v
					WHERE displayed_branch = $1 AND path = $2 AND NOT is_deleted`
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
