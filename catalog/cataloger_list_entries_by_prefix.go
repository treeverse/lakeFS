package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ListEntriesByPrefix(ctx context.Context, repo string, branch string, prefix, after string, limit int, descend bool, readUncommitted bool) ([]*Entry, bool, error) {
	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repo),
		"branch": ValidateBranchName(branch),
	}); err != nil {
		return nil, false, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repo, branch, LockTypeNone)
		if err != nil {
			return nil, err
		}

		// TODO(barak): metadata is missing
		var q string
		if readUncommitted {
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, min_commit, max_commit
					FROM entries_lineage_v
					WHERE displayed_branch = $1 AND path like $2 AND path > $3 AND NOT is_deleted
					ORDER BY path` // AND active_lineage ?
		} else {
			q = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, min_commit, max_commit
					FROM entries_lineage_committed_v
					WHERE displayed_branch = $1 AND path like $2 AND path > $3 AND NOT is_deleted
					ORDER BY path` // AND active_lineage?
		}
		args := []interface{}{branchID, db.Prefix(prefix), after}
		if descend {
			q += " DESC"
		}
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
	// has more support - we read extra one and it is the indicator for more
	hasMore := false
	if limit >= 0 && len(entries) > limit {
		entries = entries[0:limit]
		hasMore = true
	}
	return entries, hasMore, err
}
