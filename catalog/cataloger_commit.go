package catalog

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Commit(ctx context.Context, repository, branch string, message string, committer string, metadata Metadata) (*CommitLog, error) {
	if err := Validate(ValidateFields{
		{Name: "branch", IsValid: ValidateBranchName(branch)},
		{Name: "message", IsValid: ValidateCommitMessage(message)},
		{Name: "committer", IsValid: ValidateCommitter(committer)},
	}); err != nil {
		return nil, err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		branchID, err := getBranchID(tx, repository, branch, LockTypeUpdate)
		if err != nil {
			return nil, err
		}

		commitID, err := getNextCommitID(tx, branchID)
		if err != nil {
			return nil, err
		}

		committedAffected, err := commitUpdateCommittedEntriesWithMaxCommit(tx, branchID, commitID)
		if err != nil {
			return nil, err
		}

		_, err = commitDeleteUncommittedTombstones(tx, branchID, commitID)
		if err != nil {
			return nil, err
		}

		affectedTombstone, err := commitTombstones(tx, branchID, commitID)
		if err != nil {
			return nil, err
		}

		// uncommitted to committed entries
		affectedNew, err := commitEntries(tx, branchID, commitID)
		if err != nil {
			return nil, err
		}
		if (affectedNew + affectedTombstone + committedAffected) == 0 {
			return nil, ErrNothingToCommit
		}

		if err := commitIncrementCommitID(tx, branchID, commitID); err != nil {
			return nil, err
		}

		commitLog := &CommitLog{
			Committer:    committer,
			Message:      message,
			CreationDate: c.Clock.Now(),
			Metadata:     metadata,
			Parents:      nil,
		}
		if err := commitInsertCommitLog(tx, branchID, commitID, commitLog); err != nil {
			return nil, err
		}
		commitLog.Reference = MakeReference(branch, commitID)
		return commitLog, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*CommitLog), nil
}

func commitInsertCommitLog(tx db.Tx, branchID int, commitID CommitID, commitLog *CommitLog) error {
	_, err := tx.Exec(`INSERT INTO commits (branch_id, commit_id, committer, message, creation_date, metadata, merge_type) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		branchID, commitID, commitLog.Committer, commitLog.Message, commitLog.CreationDate, commitLog.Metadata, RelationTypeNone)
	return err
}

func commitIncrementCommitID(tx sqlx.Execer, branchID int, commitID CommitID) error {
	res, err := tx.Exec(`UPDATE branches SET next_commit = ($2 + 1) WHERE id = $1`,
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

func commitUpdateCommittedEntriesWithMaxCommit(tx sqlx.Execer, branchID int, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`UPDATE entries_v SET max_commit = ($2 - 1)
			WHERE branch_id = $1 AND is_committed
				AND max_commit = $3
				AND path in (SELECT path FROM entries_v WHERE branch_id = $1 AND NOT is_committed)`,
		branchID, commitID, MaxCommitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func commitDeleteUncommittedTombstones(tx sqlx.Execer, branchID int, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`DELETE FROM entries_v WHERE branch_id = $1 AND NOT is_committed AND is_tombstone AND path IN (
		SELECT path FROM entries_v WHERE branch_id = $1 AND is_committed AND max_commit = ($2 - 1))`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func commitTombstones(tx sqlx.Execer, branchID int, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`UPDATE entries_v SET min_commit = $2, max_commit = ($2 -1) WHERE branch_id = $1 AND NOT is_committed AND is_deleted`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func commitEntries(tx sqlx.Execer, branchID int, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`UPDATE entries_v SET min_commit = $2 WHERE branch_id = $1 AND NOT is_committed AND NOT is_deleted`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
