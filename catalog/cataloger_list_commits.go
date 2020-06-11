package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) ListCommits(ctx context.Context, repository, branch string, fromCommitID int, limit int) ([]*CommitLog, bool, error) {
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
		query := `SELECT b.name as branch, c.commit_id, c.committer, c.message, c.creation_date, c.metadata
			FROM commits c JOIN branches b ON b.id = c.branch_id 
			WHERE b.id = $1 AND c.commit_id > $2
			ORDER BY c.commit_id`
		args := []interface{}{branchID, fromCommitID}
		if limit >= 0 {
			query += ` LIMIT $3`
			args = append(args, limit+1)
		}

		var commits []*CommitLog
		if err := tx.Select(&commits, query, args...); err != nil {
			return nil, err
		}
		return commits, nil
	}, c.txOpts(ctx, db.ReadOnly())...)

	if err != nil {
		return nil, false, err
	}
	commits := res.([]*CommitLog)
	hasMore := paginateSlice(&commits, limit)
	return commits, hasMore, err
}
