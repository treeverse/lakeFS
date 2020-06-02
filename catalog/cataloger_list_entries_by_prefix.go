package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ListEntriesByPrefix(ctx context.Context, repo string, branch string, prefix, after string, limit int, readOptions EntryReadOptions, descend bool) ([]*Entry, bool, error) {
	if err := Validate(ValidateFields{
		"repo":   ValidateRepoName(repo),
		"branch": ValidateBranchName(branch),
	}); err != nil {
		return nil, false, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchIDByRepoBranch(tx, repo, branch)
		if err != nil {
			return nil, err
		}

		// TODO(barak): metadata is missing
		const baseQuery = `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, is_staged, min_commit, max_commit
			FROM entries_lineage_active_v
			WHERE displayed_branch = $1 AND path like $2 AND path > $3 AND `
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
		q += " ORDER BY path"
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
