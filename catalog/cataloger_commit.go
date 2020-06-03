package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Commit(ctx context.Context, repo, branch, message, committer string, metadata map[string]string) (int, error) {
	if err := Validate(ValidateFields{
		"repo":      ValidateRepoName(repo),
		"bucket":    ValidateBucketName(branch),
		"message":   ValidateCommitMessage(message),
		"committer": ValidateCommitter(committer),
	}); err != nil {
		return 0, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repo, branch, LockTypeUpdate)
		if err != nil {
			return nil, err
		}
		// get commit id
		var commitID int
		if err := tx.Get(&commitID, `SELECT next_commit FROM branches WHERE id = $1`, branchID); err != nil {
			return nil, err
		}

		// update committed entries found in the commit
		_, err = tx.Exec(`UPDATE entries_v SET max_commit = ($2 - 1)
			WHERE branch_id = $1 AND is_committed AND NOT is_deleted AND path in (
				SELECT path FROM entries_v WHERE branch_id = $1 AND NOT is_committed)`,
			branchID, commitID)
		if err != nil {
			return nil, err
		}

		// update uncommitted entries to the current commit
		res, err := tx.Exec(`UPDATE entries_v SET min_commit = $2
			WHERE branch_id = $1 AND NOT is_committed`, branchID, commitID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected == 0 {
			return nil, ErrNothingToCommit
		}

		// update next commit
		res, err = tx.Exec(`UPDATE branches SET next_commit = ($2 + 1) WHERE id = $1`, branchID, commitID)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected == 0 {
			return nil, ErrNothingToCommit
		}

		// add commit record
		creationDate := c.Clock.Now()
		// TODO(barak): missing metadata
		res, err = tx.Exec(`INSERT INTO commits (branch_id, commit_id, committer, message, creation_date, merge_type)
							VALUES ($1, $2, $3, $4, $5, $6)`,
			branchID, commitID, committer, message, creationDate, MergeTypeNone)
		if err != nil {
			return nil, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return nil, err
		} else if affected != 1 {
			return nil, ErrCommitNotFound
		}
		return commitID, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return 0, err
	}
	return res.(int), nil
}
