package catalog

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Commit(ctx context.Context, repository string, branch string, message string, committer string, metadata Metadata) (int, error) {
	if err := Validate(ValidateFields{
		"repository": ValidateRepoName(repository),
		"branch":     ValidateBranchName(branch),
		"message":    ValidateCommitMessage(message),
		"committer":  ValidateCommitter(committer),
	}); err != nil {
		return 0, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeUpdate)
		if err != nil {
			return 0, err
		}
		// get commit id
		var commitID int
		if err := tx.Get(&commitID, `SELECT next_commit FROM branches WHERE id = $1`, branchID); err != nil {
			return 0, err
		}

		// update committed entries found in the commit
		res, err := tx.Exec(`UPDATE entries_v
			SET max_commit = ($2 - 1)
			WHERE branch_id = $1 AND is_committed
				AND max_commit = ('01111111111111111111111111111111'::"bit")::integer
				AND path in (
					SELECT path
					FROM entries_v
					WHERE branch_id = $1 AND NOT is_committed)`,
			branchID, commitID)
		if err != nil {
			return 0, err
		}
		committedAffected, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}

		// TODO(barak): add test that checks that the tombstone delete works - and fix it
		// remove tombstones from entries we can set max commit
		_, err = tx.Exec(`DELETE FROM entries_v
			WHERE branch_id = $1 AND NOT is_committed AND is_deleted AND path IN (
				SELECT path FROM entries_v WHERE branch_id = $1 AND max_commit = ($2 - 1))`,
			branchID, commitID)
		if err != nil {
			return 0, err
		}

		// uncommitted delete to tombstone
		res, err = tx.Exec(`UPDATE entries_v
			SET min_commit = $2, max_commit = ($2 -1)
			WHERE branch_id = $1 AND NOT is_committed AND is_deleted`,
			branchID, commitID)
		if err != nil {
			return 0, err
		}
		affectedTombstone, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}

		// uncommitted to committed entries
		res, err = tx.Exec(`UPDATE entries_v
		SET min_commit = $2
		WHERE branch_id = $1 AND NOT is_committed AND NOT is_deleted`,
			branchID, commitID)
		if err != nil {
			return 0, err
		}
		affectedNew, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		if (affectedNew + affectedTombstone + committedAffected) == 0 {
			return 0, ErrNothingToCommit
		}

		// update next commit
		if err := commitUpdateBranchNextCommit(tx, branchID, commitID); err != nil {
			return 0, err
		}

		// add commit record
		creationDate := c.Clock.Now()
		res, err = tx.Exec(`INSERT INTO commits (branch_id, commit_id, committer, message, creation_date, metadata, merge_type)
							VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			branchID, commitID, committer, message, creationDate, metadata, MergeTypeNone)
		if err != nil {
			return 0, err
		}
		if affected, err := res.RowsAffected(); err != nil {
			return 0, err
		} else if affected != 1 {
			return 0, ErrCommitNotFound
		}
		return commitID, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return 0, err
	}
	return res.(int), nil
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
