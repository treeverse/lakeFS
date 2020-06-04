package catalog

import (
	"context"
	"errors"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ReadEntry(ctx context.Context, repo, branch, path string, readUncommitted bool) (*Entry, error) {
	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repo),
		"branch": ValidateBranchName(branch),
		"path":   ValidatePath(path),
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repo, branch, LockTypeNone)
		if err != nil {
			return nil, err
		}

		var q string
		if readUncommitted {
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, min_commit, max_commit, is_tombstone
					FROM entries_lineage_active_v
					WHERE displayed_branch = $1 AND path = $2`
		} else {
			return nil, errors.New("not implemented")
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
