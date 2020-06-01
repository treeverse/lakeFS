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
		repoID, err := repoGetIDByName(tx, repo)
		if err != nil {
			return nil, err
		}
		var branchID int
		if err := tx.Get(&branchID, `SELECT id FROM branches WHERE repository_id = $1 AND name = $2`, repoID, branch); err != nil {
			return nil, err
		}

		var isStagedCond string
		switch readOptions.EntryState {
		case EntryStateCommitted:
			isStagedCond = "is_staged IS NULL"
		case EntryStateStaged:
			isStagedCond = "is_staged = TRUE"
		case EntryStateUnstaged:
			isStagedCond = "is_staged IS NOT NULL"
		}

		// TODO(barak): metadata is missing
		prefixCond := db.Prefix(prefix)
		query := `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, is_staged, min_commit, max_commit
			FROM entries_lineage_active_v
			WHERE displayed_branch = $1 AND path like $2 AND path > $3 AND ` + isStagedCond + ` ORDER BY path`
		args := []interface{}{branchID, prefixCond, after}
		if descend {
			query += " DESC"
		}
		if limit >= 0 {
			query += " LIMIT $4"
			args = append(args, limit+1)
		}

		var entries []*Entry
		if err := tx.Select(&entries, query, args...); err != nil {
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
