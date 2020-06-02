package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ReadEntry(ctx context.Context, repo, branch, path string, readOptions EntryReadOptions) (*Entry, error) {
	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repo),
		"branch": ValidateBranchName(branch),
		"path":   ValidatePath(path),
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchIDByRepoBranch(tx, repo, branch)
		if err != nil {
			return nil, err
		}

		const baseQuery = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, is_staged, min_commit, max_commit
			FROM entries_lineage_active_v
			WHERE displayed_branch = $1 AND path = $2 AND `
		var q string
		switch readOptions.EntryState {
		case EntryStateCommitted:
			q = baseQuery + "is_staged IS NULL"
		case EntryStateStaged:
			q = baseQuery + "is_staged = TRUE"
		case EntryStateUnstaged:
			q = baseQuery + "is_staged IS NOT NULL"
		default:
			return nil, ErrInvalidReadState
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
