package catalog

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

func (c *cataloger) Merge(ctx context.Context, repository, sourceBranch, destinationBranch, committer, message string, metadata Metadata) (string, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "sourceBranch", IsValid: ValidateBranchName(sourceBranch)},
		{Name: "destinationBranch", IsValid: ValidateBranchName(destinationBranch)},
		{Name: "committer", IsValid: ValidateCommitter(committer)},
	}); err != nil {
		return "", err
	}

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		leftID, err := getBranchID(tx, repository, sourceBranch, LockTypeUpdate)
		if err != nil {
			return nil, fmt.Errorf("left branch: %w", err)
		}
		rightID, err := getBranchID(tx, repository, destinationBranch, LockTypeUpdate)
		if err != nil {
			return nil, fmt.Errorf("right branch: %w", err)
		}
		relation, err := getBranchesRelationType(tx, leftID, rightID)
		if err != nil {
			return nil, fmt.Errorf("branch relation: %w", err)
		}

		err = c.doDiffByRelation(tx, relation, leftID, rightID, 0, "")
		if err != nil {
			return nil, err
		}
		info, err := c.getDiffInformation(tx)
		if err != nil {
			return nil, err
		}
		// check for conflicts
		if info[DifferenceTypeConflict] > 0 {
			return nil, ErrConflictFound
		}
		// check for changes
		var total int
		for _, c := range info {
			total += c
		}
		if total == 0 {
			leftCommitAdvanced, err := checkZeroDiffCommit(tx, leftID, rightID)
			if err != nil {
				return nil, err
			}
			if !leftCommitAdvanced {
				return nil, ErrNoDifferenceWasFound
			}
		}

		if message == "" {
			message = formatMergeMessage(sourceBranch, destinationBranch)
		}
		return c.doMergeByRelation(tx, relation, leftID, rightID, committer, message, metadata)
	}, c.txOpts(ctx)...)
	if err != nil {
		return "", err
	}
	mergeCommitID := res.(CommitID)
	reference := MakeReference(destinationBranch, mergeCommitID)
	return reference, nil
}

// checkZeroDiffCommit - Checks if the current commit id of source branch advanced since last merge.
//		If so - a merge record must be created, even if there are no changes between branches.
func checkZeroDiffCommit(tx db.Tx, leftID, rightID int64) (bool, error) {
	leftMaxCommitID, err := getLastCommitIDByBranchID(tx, leftID)
	if err != nil {
		return false, fmt.Errorf("left branch id: %w", err)
	}
	var mergeMaxCommitID CommitID
	err = tx.Get(&mergeMaxCommitID, `SELECT DISTINCT on (branch_id) merge_source_commit 
		FROM catalog_commits
		WHERE branch_id = $1 AND merge_source_branch = $2
		ORDER BY branch_id, commit_id DESC`,
		rightID, leftID)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		return false, fmt.Errorf("max source commit id: %w", err)
	}
	if errors.Is(err, db.ErrNotFound) { // can happen only in from child merge, on the first merge
		err = tx.Get(&mergeMaxCommitID, `SELECT MIN(commit_id) FROM catalog_commits WHERE branch_id = $1`, leftID)
		if err != nil {
			return false, fmt.Errorf("min commit from left branch: %w", err)
		}
	}
	return leftMaxCommitID > mergeMaxCommitID, nil
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
	case RelationTypeFromParent:
		err = c.mergeFromParent(tx, previousMaxCommitID, nextCommitID, leftID, rightID, committer, msg, metadata)
	case RelationTypeFromChild:
		err = c.mergeFromChild(tx, previousMaxCommitID, nextCommitID, leftID, rightID, committer, msg, metadata)
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

func (c *cataloger) mergeFromParent(tx db.Tx, previousMaxCommitID, nextCommitID CommitID, parentID, childID int64, committer string, msg string, metadata Metadata) error {
	_, err := tx.Exec(`UPDATE catalog_entries SET max_commit = $2
			WHERE branch_id = $1 AND max_commit = catalog_max_commit_id() AND path in (SELECT path FROM `+diffResultsTableName+` WHERE diff_type IN ($3,$4))`,
		childID, previousMaxCommitID, DifferenceTypeRemoved, DifferenceTypeChanged)
	if err != nil {
		return err
	}

	// DifferenceTypeChanged - create entries into this commit based on parent branch
	_, err = tx.Exec(`INSERT INTO catalog_entries (branch_id,path,physical_address,creation_date,size,checksum,metadata,min_commit)
				SELECT $1,path,physical_address,creation_date,size,checksum,metadata,$2 AS min_commit
				FROM catalog_entries e
				WHERE e.ctid IN (SELECT entry_ctid FROM `+diffResultsTableName+` WHERE diff_type=$3)`,
		childID, nextCommitID, DifferenceTypeChanged)
	if err != nil {
		return err
	}
	parentLastCommitID, err := getLastCommitIDByBranchID(tx, parentID)
	if err != nil {
		return err
	}

	var parentLastLineage string
	err = tx.Get(&parentLastLineage, `SELECT DISTINCT ON (branch_id) ARRAY_TO_STRING(lineage_commits,',') FROM catalog_commits
												WHERE branch_id = $1 AND merge_type = 'from_parent' ORDER BY branch_id,commit_id DESC`, parentID)
	if err != nil && !errors.As(err, &db.ErrNotFound) {
		return err
	}

	childNewLineage := strconv.FormatInt(int64(parentLastCommitID), 10)
	if len(parentLastLineage) > 0 {
		childNewLineage += "," + parentLastLineage
	}

	_, err = tx.Exec(`INSERT INTO catalog_commits (branch_id, commit_id, previous_commit_id,committer, message, creation_date, metadata, merge_type, merge_source_branch, merge_source_commit,
                     lineage_commits)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'from_parent',$8,$9,string_to_array($10,',')::bigint[])`,
		childID, nextCommitID, previousMaxCommitID, committer, msg, c.clock.Now(), metadata, parentID, parentLastCommitID, childNewLineage)
	if err != nil {
		return err
	}
	return nil
}

func (c *cataloger) mergeFromChild(tx db.Tx, previousMaxCommitID, nextCommitID CommitID, childID int64, parentID int64, committer string, msg string, metadata Metadata) error {
	// DifferenceTypeRemoved and DifferenceTypeChanged - set max_commit the our commit for committed entries in parent branch
	_, err := tx.Exec(`UPDATE catalog_entries SET max_commit = $2
			WHERE branch_id = $1 AND max_commit = catalog_max_commit_id()
				AND path in (SELECT path FROM `+diffResultsTableName+` WHERE diff_type IN ($3,$4))`,
		parentID, previousMaxCommitID, DifferenceTypeRemoved, DifferenceTypeChanged)
	if err != nil {
		return err
	}

	// DifferenceTypeChanged or DifferenceTypeAdded - create entries into this commit based on parent branch
	_, err = tx.Exec(`INSERT INTO catalog_entries (branch_id,path,physical_address,creation_date,size,checksum,metadata,min_commit)
				SELECT $1,path,physical_address,creation_date,size,checksum,metadata,$2 AS min_commit
				FROM catalog_entries e
				WHERE e.ctid IN (SELECT entry_ctid FROM `+diffResultsTableName+` WHERE diff_type IN ($3,$4))`,
		parentID, nextCommitID, DifferenceTypeAdded, DifferenceTypeChanged)
	if err != nil {
		return err
	}
	// DifferenceTypeRemoved - create tombstones if parent "sees" those entries from lineage branches
	_, err = tx.Exec(`INSERT INTO catalog_entries (branch_id,path,physical_address,size,checksum,metadata,min_commit,max_commit)
				SELECT $1,path,'',0,'','{}',$2,0
				FROM `+diffResultsTableName+`
				WHERE diff_type=$3 AND source_branch<>$1`,
		parentID, nextCommitID, DifferenceTypeRemoved)
	if err != nil {
		return err
	}
	childLastCommitID, err := getLastCommitIDByBranchID(tx, childID)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO catalog_commits (branch_id,commit_id,previous_commit_id,committer,message,creation_date,metadata,merge_type,merge_source_branch,merge_source_commit)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'from_child',$8,$9)`,
		parentID, nextCommitID, previousMaxCommitID, committer, msg, c.clock.Now(), metadata, childID, childLastCommitID)
	return err
}

func (c *cataloger) mergeNonDirect(_ sqlx.Execer, previousMaxCommitID, nextCommitID CommitID, leftID, rightID int64, committer string, msg string, _ Metadata) error {
	c.log.WithFields(logging.Fields{
		"commit_id":      previousMaxCommitID,
		"next_commit_id": nextCommitID,
		"left_id":        leftID,
		"right_id":       rightID,
		"committer":      committer,
		"msg":            msg,
	}).Debug("Merge non direct - feature not supported")
	return ErrFeatureNotSupported
}
