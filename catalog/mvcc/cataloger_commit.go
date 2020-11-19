package mvcc

import (
	"context"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Commit(ctx context.Context, repository, branch string, message string, committer string, metadata catalog.Metadata) (*catalog.CommitLog, error) {
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
		if err = tx.GetPrimitive(&creationDate,
			`INSERT INTO catalog_commits (branch_id,commit_id,committer,message,creation_date,metadata,merge_type,previous_commit_id)
			VALUES ($1,$2,$3,$4,transaction_timestamp(),$5,$6,$7)
			RETURNING creation_date`,
			branchID, commitID, committer, message, metadata, RelationTypeNone, lastCommitID,
		); err != nil {
			return nil, err
		}

		reference := catalog.MakeReference(branch, commitID)
		parentReference := catalog.MakeReference(branch, lastCommitID)
		commitLog := &catalog.CommitLog{
			Committer:    committer,
			Message:      message,
			CreationDate: creationDate,
			Metadata:     metadata,
			Reference:    reference,
			Parents:      []string{parentReference},
		}

		for _, hook := range c.hooks.PostCommit {
			err = hook(ctx, tx, commitLog)
			if err != nil {
				// Roll tx back if a hook failed
				return nil, err
			}
		}

		return commitLog, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return res.(*catalog.CommitLog), nil
}

func commitUpdateCommittedEntriesWithMaxCommit(tx db.Tx, branchID int64, commitID catalog.CommitID) (int64, error) {
	res, err := tx.Exec(`UPDATE catalog_entries_v SET max_commit = $2
			WHERE branch_id = $1 AND is_committed
				AND max_commit = $3
				AND path in (SELECT path FROM catalog_entries_v WHERE branch_id = $1 AND NOT is_committed)`,
		branchID, commitID, catalog.MaxCommitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

func commitDeleteUncommittedTombstones(tx db.Tx, branchID int64, commitID catalog.CommitID) (int64, error) {
	res, err := tx.Exec(`DELETE FROM catalog_entries_v WHERE branch_id = $1 AND NOT is_committed AND is_tombstone AND path IN (
		SELECT path FROM catalog_entries_v WHERE branch_id = $1 AND is_committed AND max_commit = $2)`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}

func commitEntries(tx db.Tx, branchID int64, commitID catalog.CommitID) (int64, error) {
	res, err := tx.Exec(`UPDATE catalog_entries_v SET min_commit = $2 WHERE branch_id = $1 AND NOT is_committed`,
		branchID, commitID)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected(), nil
}
