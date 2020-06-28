package catalog

import (
	"context"
	"time"

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

		commitID, err := getNextCommitID(tx)
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

		commitLog, err := commitCommitLog(tx, branchID, commitID, committer, message, c.Clock.Now(), metadata)
		if err != nil {
			return nil, err
		}
		return commitLog, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*CommitLog), nil
}

func commitCommitLog(tx db.Tx, branchID int64, commitID CommitID, committer string, message string, creationDate time.Time, metadata Metadata) (*CommitLog, error) {
	commitLog := &CommitLog{
		Committer:    committer,
		Message:      message,
		CreationDate: creationDate,
		Metadata:     metadata,
		Parents:      nil,
	}
	_, err := tx.Exec(`INSERT INTO commits (branch_id, commit_id, committer, message, creation_date, metadata, merge_type) VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		branchID, commitID, committer, message, creationDate, commitLog.Metadata, RelationTypeNone)
	if err != nil {
		return nil, err
	}
	commitLog.Reference = MakeCommitReference(commitID)
	return commitLog, nil
}

func commitUpdateCommittedEntriesWithMaxCommit(tx sqlx.Execer, branchID int64, commitID CommitID) (int64, error) {
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

func commitDeleteUncommittedTombstones(tx sqlx.Execer, branchID int64, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`DELETE FROM entries_v WHERE branch_id = $1 AND NOT is_committed AND is_tombstone AND path IN (
		SELECT path FROM entries_v WHERE branch_id = $1 AND is_committed AND max_commit = ($2 - 1))`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func commitTombstones(tx sqlx.Execer, branchID int64, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`UPDATE entries_v SET min_commit = $2, max_commit = ($2 -1) WHERE branch_id = $1 AND NOT is_committed AND is_deleted`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func commitEntries(tx sqlx.Execer, branchID int64, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`UPDATE entries_v SET min_commit = $2 WHERE branch_id = $1 AND NOT is_committed AND NOT is_deleted`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
