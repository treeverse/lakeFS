package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

// TODO(barak): do we need to pass branch name also for paging
func (c *cataloger) ListRepoCommits(ctx context.Context, repo string, fromCommitID int, limit int) ([]*CommitLog, bool, error) {
	if err := Validate(ValidateFields{
		"repo": ValidateRepoName(repo),
	}); err != nil {
		return nil, false, err
	}
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		repoID, err := getRepoID(tx, repo)
		if err != nil {
			return nil, err
		}
		// TODO(barak): missing metadata
		query := `SELECT b.name as branch, c.commit_id, c.committer, c.message, c.creation_date 
			FROM commits c, branches b, repositories r 
			WHERE r.id = $1 AND r.id = b.repository_id AND b.id = c.branch_id AND c.commit_id > $2
			ORDER BY b.id, c.commit_id`
		args := []interface{}{repoID, fromCommitID}
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
	// has more support - we read extra one and it is the indicator for more
	hasMore := false
	if limit >= 0 && len(commits) > limit {
		commits = commits[0:limit]
		hasMore = true
	}
	return commits, hasMore, err
}
