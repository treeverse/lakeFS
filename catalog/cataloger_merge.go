package catalog

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

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
		leftID, err := getBranchID(tx, repository, leftBranch, LockTypeUpdate)
		if err != nil {
			return nil, err
		}
		rightID, err := getBranchID(tx, repository, rightBranch, LockTypeUpdate)
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
	// get source commit id on destination
	sourceCommitID, err := getCommitID(tx, leftID)
	if err != nil {
		return 0, err
	}
	sourceCommitID -= 1 // move next current to current source commit id

	// get destination commit id on destination
	commitID, err := getCommitID(tx, rightID)
	if err != nil {
		return 0, err
	}

	// do the merge based on the relation
	switch relation {
	case RelationTypeFromFather:
		err = c.mergeFromFather(tx, commitID, leftID, rightID)
	case RelationTypeFromSon:
		err = c.mergeFromSon(tx, commitID, leftID, rightID)
	case RelationTypeNotDirect:
		err = c.mergeNonDirect(tx, commitID, leftID, rightID)
	default:
		return 0, ErrUnsupportedRelation
	}
	if err != nil {
		return 0, err
	}

	// update next commit ID
	if err := commitIncrementCommitID(tx, rightID, commitID); err != nil {
		return 0, err
	}

	// add commit record
	if _, err := tx.Exec(`INSERT INTO commits (branch_id, commit_id, committer, message, creation_date, metadata, merge_type, merge_source_branch, merge_source_commit)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
		rightID, commitID, committer, msg, c.Clock.Now(), metadata, relation, leftID, sourceCommitID); err != nil {
		return 0, err
	}
	return commitID, nil
}

func (c *cataloger) mergeFromFather(tx sqlx.Execer, commitID CommitID, leftID int, rightID int) error {
	// set current lineages max commit to current one
	if _, err := tx.Exec(`UPDATE lineage SET max_commit=($2 - 1) WHERE branch_id=$1 AND max_commit=$3`,
		rightID, commitID, MaxCommitID); err != nil {
		return err
	}

	// copy source lineages with an effective commit
	_, err := tx.Exec(`INSERT INTO lineage (branch_id, precedence, ancestor_branch, effective_commit, min_commit)
			SELECT $1, precedence + 1, ancestor_branch, effective_commit, $3
			FROM lineage_v
			WHERE branch_id = $2 AND active_lineage`, rightID, leftID, commitID)
	if err != nil {
		return err
	}

	// DifferenceTypeRemoved and DifferenceTypeChanged - set max_commit the our commit for committed entries
	_, err = tx.Exec(`UPDATE entries SET max_commit = ($2 - 1)
			WHERE branch_id = $1 AND max_commit = $3
				AND path in (SELECT path FROM `+diffResultsTableName+` WHERE diff_type IN ($4,$5))`,
		rightID, commitID, MaxCommitID, DifferenceTypeRemoved, DifferenceTypeChanged)
	if err != nil {
		return err
	}

	// DifferenceTypeChanged - create entries into this commit based on father branch
	_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,creation_date,size,checksum,metadata,min_commit)
				SELECT $1,path,physical_address,creation_date,size,checksum,metadata,$2 AS min_commit
				FROM entries e
				WHERE e.ctid IN (SELECT entry_ctid FROM `+diffResultsTableName+` WHERE diff_type=$3)`,
		rightID, commitID, DifferenceTypeChanged)
	return err
}

func (c *cataloger) mergeFromSon(tx sqlx.Execer, commitID CommitID, _ int, rightID int) error {
	// DifferenceTypeRemoved and DifferenceTypeChanged - set max_commit the our commit for committed entries
	_, err := tx.Exec(`UPDATE entries SET max_commit = ($2 - 1)
			WHERE branch_id = $1 AND max_commit = $3
				AND path in (SELECT path FROM `+diffResultsTableName+` WHERE diff_type IN ($4,$5))`,
		rightID, commitID, MaxCommitID, DifferenceTypeRemoved, DifferenceTypeChanged)
	if err != nil {
		return err
	}

	// DifferenceTypeChanged or DifferenceTypeAdded - create entries into this commit based on father branch
	_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,creation_date,size,checksum,metadata,min_commit)
				SELECT $1,path,physical_address,creation_date,size,checksum,metadata,$2 AS min_commit
				FROM entries e
				WHERE e.ctid IN (SELECT entry_ctid FROM `+diffResultsTableName+` WHERE diff_type IN ($3,$4))`,
		rightID, commitID, DifferenceTypeAdded, DifferenceTypeChanged)
	if err != nil {
		return err
	}
	// DifferenceTypeRemoved - create tombstones if source branch is not our destination
	_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,size,checksum,metadata,min_commit,max_commit)
				SELECT $1,path,'',0,'','{}',$2,0
				FROM `+diffResultsTableName+`
				WHERE diff_type=$3 AND source_branch<>$4`,
		rightID, commitID, DifferenceTypeRemoved, rightID)
	return err
}

func (c *cataloger) mergeNonDirect(tx sqlx.Execer, commitID CommitID, leftID int, rightID int) error {
	panic("not implemented - Someday is not a day of the week")
}
