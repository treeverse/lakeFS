package catalog

import (
	"context"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Commit(ctx context.Context, repo, branch, message, committer string, metadata map[string]string, unstaged bool) (*Commit, error) {
	if err := Validate(ValidateFields{
		"repo":      ValidateRepoName(repo),
		"bucket":    ValidateBucketName(branch),
		"message":   ValidateCommitMessage(message),
		"committer": ValidateCommitter(committer),
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		/*
			branchID, err := getBranchID(tx, repo, branch, LockTypeNone)
			if err != nil {
				return nil, err
			}
			// make sure all objects are marked with next commit id
				res, err := tx.Exec(`UPDATE TABLE entries
					WHERE branch_id = $1 AND is_staged IS NOT NULL
					SET is_staged = NULL, min_commit = (SELECT next_commit FROM branches WHERE branch_id = $1)`,
					branchID)

					var entriesCount int
					if err := tx.Get(&entriesCount, entriesCountSql); err != nil {
						return nil, err
					}
					if entriesCount == 0 {
						return nil, ErrNothingToCommit
					}
						// update branch's next commit
						res, err := tx.Exec(`UPDATE branches

						// mark entries to be part of the commit
						// commit record
						creationDate := c.Clock.Now()
						res, err := tx.Exec(`INSERT INTO commits (branch_id, commit_number, committer, message, creation_date, metadata)
							VALUES ($1, $2, $3, $4, $5, $6)`,
							branchID, commitID, committer, message, creationDate, metadata)
						if err != nil {
							return nil, err
						}
						if affected, err := res.RowsAffected(); err != nil {
							return nil, err
						} else if affected != 1 {
							return nil, ErrCommitNotFound
						}
		*/
		var commit Commit
		return &commit, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*Commit), nil
}
