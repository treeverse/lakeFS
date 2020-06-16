package catalog

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Merge(ctx context.Context, repository, leftBranch, rightBranch string, committer string, metadata Metadata) (*MergeResult, error) {
	if err := Validate(ValidateFields{
		"repository":       ValidateRepositoryName(repository),
		"left branch":      ValidateBranchName(leftBranch),
		"right branch":     ValidateBranchName(rightBranch),
		"committer branch": ValidateCommitter(committer),
	}); err != nil {
		return nil, err
	}

	var result *MergeResult
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		leftID, err := getBranchID(tx, repository, leftBranch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		rightID, err := getBranchID(tx, repository, rightBranch, LockTypeShare)
		if err != nil {
			return nil, err
		}
		relation, err := getBranchesRelationType(tx, leftID, rightID)
		if err != nil {
			return nil, err
		}

		differences, err := c.doDiffByRelation(tx, relation, leftID, rightID)
		if err != nil {
			return nil, err
		}
		result = &MergeResult{
			Differences: differences,
		}
		diffCounts := result.Differences.CountByType()
		if diffCounts[DifferenceTypeConflict] > 0 {
			return nil, ErrConflictFound
		}
		if len(diffCounts) == 0 {
			return nil, ErrNoDifferenceWasFound
		}

		commitMsg := formatMergeMessage(leftBranch, rightBranch)
		result.CommitID, err = c.doMergeByRelation(tx, relation, leftID, rightID, committer, commitMsg, metadata)
		return nil, err
	}, c.txOpts(ctx)...)
	return result, err
}

func formatMergeMessage(leftBranch string, rightBranch string) string {
	return fmt.Sprintf("Merge '%s' into '%s'", leftBranch, rightBranch)
}

func (c *cataloger) doMergeByRelation(tx db.Tx, relation RelationType, leftID int, rightID int, committer string, msg string, metadata Metadata) (CommitID, error) {
	switch relation {
	case RelationTypeFromFather:
		return c.mergeFromFather(tx, leftID, rightID, committer, msg, metadata)
	case RelationTypeFromSon:
		return c.mergeFromSun(tx, leftID, rightID, committer, msg, metadata)
	case RelationTypeNotDirect:
		return c.mergeNonDirect(tx, leftID, rightID, committer, msg, metadata)
	default:
		return 0, nil
	}
}

func (c *cataloger) mergeFromFather(tx db.Tx, leftID int, rightID int, committer string, msg string, metadata Metadata) (CommitID, error) {
	// get commit id on destination
	commitID, err := getCommitID(tx, rightID)
	if err != nil {
		return 0, err
	}

	// set current lineages max commit to current one
	if _, err := tx.Exec(`UPDATE lineage SET max_commit=($2 - 1) WHERE branch_id=$1 AND max_commit=$3`, rightID, commitID, MaxCommitID); err != nil {
		return 0, err
	}

	// copy source lineages with an effective commit
	_, err = tx.Exec(`INSERT INTO lineage (branch_id, precedence, ancestor_branch, effective_commit, min_commit)
			SELECT $1, precedence + 1, ancestor_branch, effective_commit, $3
			FROM lineage_v
			WHERE branch_id = $2`, rightID, leftID, commitID)
	if err != nil {
		return 0, err
	}

	// DifferenceTypeRemoved and DifferenceTypeChanged - set max_commit the our commit for committed entries
	_, err = tx.Exec(`UPDATE entries SET max_commit = ($2 - 1)
			WHERE branch_id = $1 AND max_commit = $3
				AND path in (SELECT path FROM `+diffResultsTableName+` WHERE diff_type IN ($4,$5))`,
		rightID, commitID, MaxCommitID, DifferenceTypeRemoved, DifferenceTypeChanged)
	if err != nil {
		return 0, err
	}

	// DifferenceTypeChanged - create entries into this commit based on father branch
	_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,creation_date,size,checksum,metadata,min_commit)
				SELECT $1,path,physical_address,creation_date,size,checksum,metadata,$2 AS min_commit
				FROM entries e
				WHERE e.ctid IN (SELECT entry_ctid FROM `+diffResultsTableName+` WHERE diff_type=$3)`,
		rightID, commitID, DifferenceTypeChanged)
	if err != nil {
		return 0, err
	}

	// update next commit ID
	if err := commitIncrementCommitID(tx, rightID, commitID); err != nil {
		return 0, err
	}

	// add commit record
	if err := commitInsertCommitRecord(tx, rightID, commitID, committer, msg, metadata, c.Clock.Now()); err != nil {
		return 0, err
	}

	return commitID, nil
}

func (c *cataloger) mergeFromSun(tx db.Tx, leftID int, rightID int, committer string, msg string, metadata Metadata) (CommitID, error) {
	panic("implement")
}

func (c *cataloger) mergeNonDirect(tx db.Tx, leftID int, rightID int, committer string, msg string, metadata Metadata) (CommitID, error) {
	panic("implement")
}
