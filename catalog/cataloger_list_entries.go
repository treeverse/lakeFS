package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

// TODO(barak): add delimiter support - return entry and common prefix entries

func (c *cataloger) ListEntries(ctx context.Context, repository string, branch string, prefix, after string, limit int, readUncommitted bool) ([]*Entry, bool, error) {
	if err := Validate(ValidateFields{
		"repository": ValidateRepoName(repository),
		"branch":     ValidateBranchName(branch),
	}); err != nil {
		return nil, false, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeNone)
		if err != nil {
			return nil, err
		}

		var q string
		if readUncommitted {
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, metadata, min_commit, max_commit
					FROM entries_lineage_v
					WHERE displayed_branch = $1 AND path like $2 AND path > $3 AND NOT is_deleted
					ORDER BY path`
		} else {
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, metadata, min_commit, max_commit
					FROM entries_lineage_committed_v
					WHERE displayed_branch = $1 AND path like $2 AND path > $3 AND NOT is_deleted
					ORDER BY path`
		}
		args := []interface{}{branchID, db.Prefix(prefix), after}
		if limit >= 0 {
			q += " LIMIT $4"
			args = append(args, limit+1)
		}

		var entries []*Entry
		if err := tx.Select(&entries, q, args...); err != nil {
			return nil, err
		}

		return entries, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	entries := res.([]*Entry)
	hasMore := paginateSlice(&entries, limit)
	return entries, hasMore, err
}
