package catalog

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jmoiron/sqlx"

	"github.com/treeverse/lakefs/db"
)

func (c *cataloger) Merge(ctx context.Context, repository, leftBranch, rightBranch string, committer string, message string, metadata Metadata) (*MergeResult, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "leftBranch", IsValid: ValidateBranchName(leftBranch)},
		{Name: "rightBranch", IsValid: ValidateBranchName(rightBranch)},
		{Name: "committer", IsValid: ValidateCommitter(committer)},
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

		if message == "" {
			message = formatMergeMessage(leftBranch, rightBranch)
		}
		commitID, err := c.doMergeByRelation(tx, relation, leftID, rightID, committer, message, metadata)
		if err != nil {
			return nil, err
		}
		result.Reference = MakeReference(rightBranch, commitID)
		return nil, err
	}, c.txOpts(ctx)...)
	return result, err
}

func formatMergeMessage(leftBranch string, rightBranch string) string {
	return fmt.Sprintf("Merge '%s' into '%s'", leftBranch, rightBranch)
}

func (c *cataloger) doMergeByRelation(tx db.Tx, relation RelationType, leftID, rightID int64, committer string, msg string, metadata Metadata) (CommitID, error) {
	nextCommitID, err := getNextCommitID(tx)
	if err != nil {
		return 0, err
	}

	// do the merge based on the relation
	previousMaxCommitID, err := getLastCommitIDByBranchID(tx, rightID)
	if err != nil {
		return 0, err
	}
	switch relation {
	case RelationTypeFromFather:
		err = c.mergeFromFather(tx, previousMaxCommitID, nextCommitID, leftID, rightID, committer, msg, metadata)
	case RelationTypeFromSon:
		err = c.mergeFromSon(tx, previousMaxCommitID, nextCommitID, leftID, rightID, committer, msg, metadata)
	case RelationTypeNotDirect:
		err = c.mergeNonDirect(tx, previousMaxCommitID, nextCommitID, leftID, rightID, committer, msg, metadata)
	default:
		return 0, ErrUnsupportedRelation
	}
	if err != nil {
		return 0, err
	}
	return nextCommitID, nil
}

func (c *cataloger) mergeFromFather(tx db.Tx, previousMaxCommitID, nextCommitID CommitID, fatherID, sonID int64, committer string, msg string, metadata Metadata) error {
	_, err := tx.Exec(`UPDATE entries SET max_commit = $2
			WHERE branch_id = $1 AND max_commit = $3 AND path in (SELECT path FROM `+diffResultsTableName+` WHERE diff_type IN ($4,$5))`,
		sonID, previousMaxCommitID, MaxCommitID, DifferenceTypeRemoved, DifferenceTypeChanged)
	if err != nil {
		return err
	}

	// DifferenceTypeChanged - create entries into this commit based on father branch
	_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,creation_date,size,checksum,metadata,min_commit)
				SELECT $1,path,physical_address,creation_date,size,checksum,metadata,$2 AS min_commit
				FROM entries e
				WHERE e.ctid IN (SELECT entry_ctid FROM `+diffResultsTableName+` WHERE diff_type=$3)`,
		sonID, nextCommitID, DifferenceTypeChanged)
	if err != nil {
		return err
	}
	fatherLastCommitID, err := getLastCommitIDByBranchID(tx, fatherID)
	if err != nil {
		return err
	}

	var fatherLastLineage, sonNewLineage string
	err = tx.Get(&fatherLastLineage, `SELECT DISTINCT ON (branch_id) ARRAY_TO_STRING(lineage_commits,',') FROM commits 
												WHERE branch_id = $1 AND merge_type = 'from_father' ORDER BY branch_id,commit_id DESC`, fatherID)
	if err != nil && !errors.As(err, &db.ErrNotFound) {
		return err
	}

	if len(fatherLastLineage) > 0 {
		sonNewLineage = strconv.FormatInt(int64(fatherLastCommitID), 10) + "," + fatherLastLineage
	} else {
		sonNewLineage = strconv.FormatInt(int64(fatherLastCommitID), 10)
	}

	_, err = tx.Exec(`INSERT INTO commits (branch_id, commit_id, previous_commit_id,committer, message, creation_date, metadata, merge_type, merge_source_branch, merge_source_commit,
                     lineage_commits)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'from_father',$8,$9,string_to_array($10,',')::bigint[])`,
		sonID, nextCommitID, previousMaxCommitID, committer, msg, c.clock.Now(), metadata, fatherID, fatherLastCommitID, sonNewLineage)
	if err != nil {
		return err
	}
	return nil
}

func (c *cataloger) mergeFromSon(tx db.Tx, previousMaxCommitID, nextCommitID CommitID, sonID int64, fatherID int64, committer string, msg string, metadata Metadata) error {
	// DifferenceTypeRemoved and DifferenceTypeChanged - set max_commit the our commit for committed entries in father branch
	_, err := tx.Exec(`UPDATE entries SET max_commit = $2
			WHERE branch_id = $1 AND max_commit = max_commit_id()
				AND path in (SELECT path FROM `+diffResultsTableName+` WHERE diff_type IN ($3,$4))`,
		fatherID, previousMaxCommitID, DifferenceTypeRemoved, DifferenceTypeChanged)
	if err != nil {
		return err
	}

	// DifferenceTypeChanged or DifferenceTypeAdded - create entries into this commit based on father branch
	_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,creation_date,size,checksum,metadata,min_commit)
				SELECT $1,path,physical_address,creation_date,size,checksum,metadata,$2 AS min_commit
				FROM entries e
				WHERE e.ctid IN (SELECT entry_ctid FROM `+diffResultsTableName+` WHERE diff_type IN ($3,$4))`,
		fatherID, nextCommitID, DifferenceTypeAdded, DifferenceTypeChanged)
	if err != nil {
		return err
	}
	// DifferenceTypeRemoved - create tombstones if father "sees" those entries from lineage branches
	_, err = tx.Exec(`INSERT INTO entries (branch_id,path,physical_address,size,checksum,metadata,min_commit,max_commit)
				SELECT $1,path,'',0,'','{}',$2,0
				FROM `+diffResultsTableName+`
				WHERE diff_type=$3 AND source_branch<>$1`,
		fatherID, nextCommitID, DifferenceTypeRemoved)
	if err != nil {
		return err
	}
	sonLastCommitID, err := getLastCommitIDByBranchID(tx, sonID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO commits (branch_id, commit_id, previous_commit_id,committer, message, creation_date, metadata, merge_type, merge_source_branch, merge_source_commit)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'from_son',$8,$9)`,
		fatherID, nextCommitID, previousMaxCommitID, committer, msg, c.clock.Now(), metadata, sonID, sonLastCommitID)
	return err
}

func (c *cataloger) mergeNonDirect(tx sqlx.Execer, previousMaxCommitID, nextCommitID CommitID, leftID, rightID int64, committer string, msg string, metadata Metadata) error {
	panic("not implemented - Someday is not a day of the week")
}
