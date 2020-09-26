package catalog

import (
	"context"
	"fmt"
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
			return nil, fmt.Errorf("get branch id: %w", err)
		}

		lastCommitID, err := getLastCommitIDByBranchID(tx, branchID)
		if err != nil {
			return nil, fmt.Errorf("last commit id: %w", err)
		}

		committedAffected, err := commitUpdateCommittedEntriesWithMaxCommit(tx, branchID, lastCommitID)
		if err != nil {
			return nil, fmt.Errorf("update commit entries: %w", err)
		}

		_, err = commitDeleteUncommittedTombstones(tx, branchID, lastCommitID)
		if err != nil {
			return nil, fmt.Errorf("delete uncommitted tombstones: %w", err)
		}

		// uncommitted to committed entries
		commitID, err := getNextCommitID(tx)
		if err != nil {
			return nil, fmt.Errorf("next commit id: %w", err)
		}

		// commit entries (include the tombstones)
		affectedNew, err := commitEntries(tx, branchID, commitID)
		if err != nil {
			return nil, fmt.Errorf("commit entries: %w", err)
		}
		if (affectedNew + committedAffected) == 0 {
			return nil, ErrNothingToCommit
		}

		// insert commit record
		var creationDate time.Time
		if err = tx.Get(&creationDate,
			`INSERT INTO catalog_commits (branch_id,commit_id,committer,message,creation_date,metadata,merge_type,previous_commit_id)
			VALUES ($1,$2,$3,$4,transaction_timestamp(),$5,$6,$7)
			RETURNING creation_date`,
			branchID, commitID, committer, message, metadata, RelationTypeNone, lastCommitID,
		); err != nil {
			return nil, err
		}
		reference := MakeReference(branch, commitID)
		parentReference := MakeReference(branch, lastCommitID)
		commitLog := &CommitLog{
			Committer:    committer,
			Message:      message,
			CreationDate: creationDate,
			Metadata:     metadata,
			Reference:    reference,
			Parents:      []string{parentReference},
		}
		return commitLog, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*CommitLog), nil
}

func commitUpdateCommittedEntriesWithMaxCommit(tx sqlx.Execer, branchID int64, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`UPDATE catalog_entries_v SET max_commit = $2
			WHERE branch_id = $1 AND is_committed
				AND max_commit = catalog_max_commit_id()
				AND path in (SELECT path FROM catalog_entries_v WHERE branch_id = $1 AND NOT is_committed)`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func commitDeleteUncommittedTombstones(tx sqlx.Execer, branchID int64, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`DELETE FROM catalog_entries_v WHERE branch_id = $1 AND NOT is_committed AND is_tombstone AND path IN (
		SELECT path FROM catalog_entries_v WHERE branch_id = $1 AND is_committed AND max_commit = $2)`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func commitEntries(tx sqlx.Execer, branchID int64, commitID CommitID) (int64, error) {
	res, err := tx.Exec(`UPDATE catalog_entries_v SET min_commit = $2 WHERE branch_id = $1 AND NOT is_committed`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
