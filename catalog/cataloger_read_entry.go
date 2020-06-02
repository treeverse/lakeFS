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
		_, branchID, err := getRepoAndBranchByName(tx, repo, branch)
		if err != nil {
			return nil, err
		}

		// TODO(barak): missing metadata
		query := `SELECT displayed_branch as branch_id, path, physical_address, creation_date, size, checksum, is_staged, min_commit, max_commit
			FROM entries_lineage_active_v
			WHERE displayed_branch = $1 AND path = $2 AND ` +
			readOptionsAsStagedCondition(readOptions)

		var ent Entry
		if err := tx.Get(&ent, query, branchID, path); err != nil {
			return nil, err
		}
		return &ent, nil
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return res.(*Entry), nil
}
