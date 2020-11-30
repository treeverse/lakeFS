package mvcc

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/catalog"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) RollbackCommit(ctx context.Context, repository, reference string) error {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "reference", IsValid: ValidateReference(reference)},
	}); err != nil {
		return err
	}

	ref, err := ParseRef(reference)
	if err != nil {
		return err
	}
	if ref.CommitID <= UncommittedID {
		return catalog.ErrInvalidReference
	}

	_, err = c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// extract branch id from reference
		branchID, err := getBranchID(tx, repository, ref.Branch, LockTypeUpdate)
		if err != nil {
			return nil, err
		}

		// validate no child branch point to parent commit
		var count int
		err = tx.GetPrimitive(&count, `SELECT COUNT(*) from catalog_commits
			WHERE merge_source_branch = $1 AND merge_source_commit > $2 AND merge_type = 'from_parent'`,
			branchID, ref.CommitID)
		if err != nil {
			return nil, fmt.Errorf("check merge with branch: %w", err)
		}
		if count > 0 {
			return nil, catalog.ErrRollbackWithActiveBranch
		}

		// delete all commits after this commit on this branch
		_, err = tx.Exec(`DELETE FROM catalog_commits WHERE branch_id = $1 AND commit_id > $2`,
			branchID, ref.CommitID)
		if err != nil {
			return nil, fmt.Errorf("delete commits on branch %d, after commit %d: %w", branchID, ref.CommitID, err)
		}

		// delete all entries created after this commit
		_, err = tx.Exec(`DELETE FROM catalog_entries WHERE branch_id = $1 AND min_commit > $2`,
			branchID, ref.CommitID)
		if err != nil {
			return nil, fmt.Errorf("delete entries %d, after min commit %d: %w", branchID, ref.CommitID, err)
		}

		// update max_commit to infinite
		_, err = tx.Exec(`UPDATE catalog_entries SET max_commit = $1 WHERE branch_id = $2 AND max_commit > $3`,
			MaxCommitID, branchID, ref.CommitID)
		if err != nil {
			return nil, fmt.Errorf("clear entries %d, max commit %d: %w", branchID, ref.CommitID, err)
		}
		return nil, nil
	}, c.txOpts(ctx, db.ReadCommitted())...)
	return err
}
