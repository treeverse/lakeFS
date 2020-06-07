package catalog

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Commit(ctx context.Context, repo, branch, message, committer string, metadata Metadata) (int, error) {
	if err := Validate(ValidateFields{
		"repo":      ValidateRepoName(repo),
		"branch":    ValidateBranchName(branch),
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
		if err := commitUpdatePrevCommittedMaxCommit(tx, branchID, commitID); err != nil {
			return nil, err
		}

		// update uncommitted entries to the current commit
		if err := commitUpdateCommitLevels(tx, branchID, commitID); err != nil {
			return nil, err
		}

		// update next commit
		if err := commitUpdateBranchNextCommit(tx, branchID, commitID); err != nil {
			return nil, err
		}

		// add commit record
		creationDate := c.Clock.Now()
		res, err := tx.Exec(`INSERT INTO commits (branch_id, commit_id, committer, message, creation_date, metadata, merge_type)
							VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			branchID, commitID, committer, message, creationDate, metadata, MergeTypeNone)
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

func commitUpdatePrevCommittedMaxCommit(tx sqlx.Execer, branchID int, commitID int) error {
	_, err := tx.Exec(`UPDATE entries_v
		SET max_commit = ($2 - 1)
		WHERE branch_id = $1 AND is_committed AND NOT is_deleted AND path in (
			SELECT path
			FROM entries_v
			WHERE branch_id = $1 AND NOT is_committed)`,
		branchID, commitID)
	return err
}

func commitUpdateCommitLevels(tx sqlx.Execer, branchID int, commitID int) error {
	// remove tombstones for entries we can set max commit
	_, err := tx.Exec(`DELETE FROM entries_v
		WHERE branch_id = $1 AND NOT is_committed AND is_deleted AND path IN (
			SELECT path FROM entries_v WHERE branch_id = $1 AND NOT is_committed)`,
		branchID)
	if err != nil {
		return err
	}

	// set commit levels for deleted entries
	res, err := tx.Exec(`UPDATE entries_v
		SET min_commit = $2, max_commit = ($2 -1)
		WHERE branch_id = $1 AND NOT is_committed AND is_deleted`,
		branchID, commitID)
	if err != nil {
		return err
	}
	affectedTombstone, err := res.RowsAffected()
	if err != nil {
		return err
	}

	// set commit levels for non-deleted entries
	res, err = tx.Exec(`UPDATE entries_v
		SET min_commit = $2
		WHERE branch_id = $1 AND NOT is_committed AND NOT is_deleted`,
		branchID, commitID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if (affected + affectedTombstone) == 0 {
		return ErrNothingToCommit
	}
	return nil
}

func commitUpdateBranchNextCommit(tx sqlx.Execer, branchID int, commitID int) error {
	res, err := tx.Exec(`UPDATE branches
		SET next_commit = ($2 + 1) 
		WHERE id = $1`,
		branchID, commitID)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err != nil {
		return err
	} else if affected == 0 {
		return ErrNothingToCommit
	}
	return nil
}
