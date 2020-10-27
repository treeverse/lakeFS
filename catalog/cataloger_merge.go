package catalog

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	sq "github.com/Masterminds/squirrel"

	"github.com/jmoiron/sqlx"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const (
	MergeBatchSize = 128
)

type mergeBatchFunc func(tx db.Tx, mergeBatch mergeBatchType, previousMaxCommitID, nextCommitID CommitID, parentID, childID int64) error
type mergeBatchType []*diffResultRecord

// Merge perform diff between two branches (left and right), apply changes on right branch and commit
// It uses the cataloger diff internal API to produce a temporary table that we delete at the end of a successful merge
// the table holds entry ctid to reference entries in case of changed/added and source branch in case of delete.
// That information is used to address cases where we need to create new entry or tombstone as part of the merge
func (c *cataloger) Merge(ctx context.Context, repository, leftBranch, rightBranch, committer, message string, metadata Metadata) (*MergeResult, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "leftBranch", IsValid: ValidateBranchName(leftBranch)},
		{Name: "rightBranch", IsValid: ValidateBranchName(rightBranch)},
		{Name: "committer", IsValid: ValidateCommitter(committer)},
	}); err != nil {
		return nil, err
	}

	ctx, cancel := c.withDiffResultsContext(ctx)
	defer cancel()

	mergeResult := &MergeResult{}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		leftID, err := getBranchID(tx, repository, leftBranch, LockTypeUpdate)
		if err != nil {
			return nil, fmt.Errorf("left branch: %w", err)
		}
		rightID, err := getBranchID(tx, repository, rightBranch, LockTypeUpdate)
		if err != nil {
			return nil, fmt.Errorf("right branch: %w", err)
		}

		params := &doDiffParams{
			Repository:    repository,
			LeftCommitID:  CommittedID,
			LeftBranchID:  leftID,
			RightCommitID: UncommittedID,
			RightBranchID: rightID,
			DiffParams: DiffParams{
				Limit: -1,
			},
		}

		relation, err := c.getRefsRelationType(tx, params)
		if err != nil {
			return nil, fmt.Errorf("get refs relation: %w", err)
		}
		if relation == RelationTypeSame {
			return nil, fmt.Errorf("merge from the same branch: %w", ErrOperationNotPermitted)
		}
		//err = c.doDiff(ctx, tx, params)
		//if err != nil {
		//	return nil, err
		//}
		scanner, err := c.newDiffScanner(tx, params)
		if err != nil {
			return nil, err
		}
		var mergeFunc mergeBatchFunc
		switch relation {
		case RelationTypeFromParent:
			mergeFunc = c.mergeBatchFromParent
		case RelationTypeFromChild:
			mergeFunc = c.mergeBatchFromChild
		default:
			return nil, fmt.Errorf("unknown relation: %w", ErrOperationNotPermitted)
		}
		nextCommitID, err := getNextCommitID(tx)
		if err != nil {
			return 0, err
		}
		// do the merge based on the relation
		previousMaxCommitID, err := getLastCommitIDByBranchID(tx, rightID)
		if err != nil {
			return 0, err
		}
		differences := make(mergeBatchType, 0, MergeBatchSize)
		for scanner.Next() {
			v := scanner.Value()
			if v.DiffType == DifferenceTypeConflict {
				panic("got conflict")
			}
			differences = append(differences, v)
			if len(differences) >= MergeBatchSize {
				err = mergeFunc(tx, differences, previousMaxCommitID, nextCommitID, leftID, rightID)
				if err != nil {
					panic(err)
				}
			}
			differences = differences[:0]
			//if clearChecksumField {
			//	d.Entry.Checksum = ""
			//}
		}
		err = scanner.Error()
		if err != nil {
			return nil, err
		}
		err = mergeFunc(tx, differences, previousMaxCommitID, nextCommitID, leftID, rightID)
		if err != nil {
			return nil, err
		}
		if message == "" {
			message = fmt.Sprintf("Merge '%s' into '%s'", leftBranch, rightBranch)
		}
		err = InsertMergeCommit(tx, relation, leftID, rightID, nextCommitID, previousMaxCommitID, committer, message, metadata)
		//*************************************************
		mergeResult.Summary = scanner.diffSummary

		// check for conflicts
		if mergeResult.Summary[DifferenceTypeConflict] > 0 {
			return nil, ErrConflictFound
		}
		// check for changes
		var total int
		for _, c := range mergeResult.Summary {
			total += c
		}
		if total == 0 {
			commitDifferences, err := hasCommitDifferences(tx, leftID, rightID)
			if err != nil {
				return nil, err
			}
			if !commitDifferences {
				return nil, ErrNoDifferenceWasFound
			}
		}

		if message == "" {
			message = formatMergeMessage(leftBranch, rightBranch)
		}
		commitID, err := c.doMergeByRelation(ctx, tx, relation, leftID, rightID, committer, message, metadata)
		if err != nil {
			return nil, err
		}
		mergeResult.Reference = MakeReference(rightBranch, commitID)
		return nil, nil
	}, c.txOpts(ctx)...)
	return mergeResult, err
}

// hasCommitDifferences - Checks if the current commit id of target or source branch advanced since last merge
func hasCommitDifferences(tx db.Tx, leftID, rightID int64) (bool, error) {
	var hasCommitDifferences bool
	mergeCommitsQuery := `select right_merge_commit < max_right_commit or left_merge_commit < max_left_commit from
		(select distinct on (branch_id) commit_id as right_merge_commit, merge_source_commit as left_merge_commit,
		(select max(commit_id) from catalog_commits where branch_id=$1)as max_right_commit,
		(select max(commit_id) from catalog_commits where branch_id=$2)as max_left_commit
		from catalog_commits where branch_id = $1 and merge_source_branch = $2
		order by branch_id,commit_id desc) t`
	err := tx.Get(&hasCommitDifferences, mergeCommitsQuery, rightID, leftID)
	if errors.Is(err, db.ErrNotFound) {
		// not found errors indicate there is no merge record for this relation
		//  a parent to child merge record is written when the branch is created,
		// so this may happen only on first child to parent merge.
		// in this case - a check is done if any commits where done to child.
		const checkChildCommitsQuery = "select exists(select * from catalog_commits where branch_id = $1 and merge_type='none')"
		err = tx.Get(&hasCommitDifferences, checkChildCommitsQuery, leftID)
	}
	if err != nil {
		return false, fmt.Errorf("has commit difference: %w", err)
	}
	return hasCommitDifferences, nil
}

//func (c *cataloger) doMergeByRelation(ctx context.Context, tx db.Tx, relation RelationType, leftID, rightID int64, committer string, msg string, metadata Metadata) (CommitID, error) {
//
//	switch relation {
//	case RelationTypeFromParent:
//		err = c.mergeFromParent(ctx, tx, previousMaxCommitID, nextCommitID, leftID, rightID, committer, msg, metadata)
//	case RelationTypeFromChild:
//		err = c.mergeFromChild(ctx, tx, previousMaxCommitID, nextCommitID, leftID, rightID, committer, msg, metadata)
//	case RelationTypeNotDirect:
//		err = c.mergeNonDirect(ctx, tx, previousMaxCommitID, nextCommitID, leftID, rightID, committer, msg, metadata)
//	default:
//		return 0, ErrUnsupportedRelation
//	}
//	if err != nil {
//		return 0, err
//	}
//	return nextCommitID, nil
//}

func (cc *cataloger) mergeBatchFromParent(tx db.Tx, mergeBatch mergeBatchType, previousMaxCommitID, nextCommitID CommitID, parentID, childID int64) error {
	paths := make([]string, 0, MergeBatchSize)
	ctidArray := make([]string, 0, MergeBatchSize)
	for _, diffRec := range mergeBatch {
		if diffRec.DiffType == DifferenceTypeRemoved || diffRec.DiffType == DifferenceTypeChanged {
			paths = append(paths, diffRec.Entry.Path)
		}
		if (diffRec.DiffType == DifferenceTypeAdded || diffRec.DiffType == DifferenceTypeChanged) &&
			diffRec.EntryCtid != nil {
			ctidArray = append(ctidArray, *diffRec.EntryCtid)
		}
	}
	if len(paths) > 0 {
		setMaxCommit := sq.Update("catalog_entries").
			Set("maxCommit", previousMaxCommitID).
			Where("branch_id = ? and max_commit = ?", childID, MaxCommitID).
			Where(sq.Eq{"path": paths})
		sql, args, err := setMaxCommit.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return err
		}
		_, err = tx.Exec(sql, args...)
		if err != nil {
			return err
		}
	}
	if len(ctidArray) > 0 {
		internalSelect := sq.Select(int64Str(childID), "path", "physical_address", "creation_date", "size", "checksum", "metadata", int64Str(int64(nextCommitID))).
			From("catalog_entries").
			Where(sq.Eq{"ctid": ctidArray})
		copyEntries := sq.Insert("catalog_entries").
			Columns("branch_id", "path", "physical_address", "creation_date", "size", "checksum", "metadata", "min_commit").
			Select(internalSelect)
		sql, args, err := copyEntries.PlaceholderFormat(sq.Dollar).ToSql()
		if err != nil {
			return err
		}
		_, err = tx.Exec(sql, args...)
		if err != nil {
			return err
		}
	}
	return nil
}

func InsertMergeCommit(tx db.Tx, relation RelationType, leftID int64, rightID int64, nextCommitID CommitID, previousMaxCommitID CommitID, committer string, msg string, metadata Metadata) error {
	var err error
	var childNewLineage *string
	var parentLastLineage string
	leftLastCommitID, err := getLastCommitIDByBranchID(tx, leftID)
	if err != nil {
		return err
	}
	if relation == RelationTypeFromParent {
		err = tx.Get(&parentLastLineage, `SELECT DISTINCT ON (branch_id) ARRAY_TO_STRING(lineage_commits,',') FROM catalog_commits
												WHERE branch_id = $1 AND merge_type = 'from_parent' ORDER BY branch_id,commit_id DESC`, leftID)
		if err != nil && !errors.As(err, &db.ErrNotFound) {
			return err
		}
		*childNewLineage = int64Str(int64(leftLastCommitID))
		if len(parentLastLineage) > 0 {
			*childNewLineage += "," + parentLastLineage
		}
	}
	_, err = tx.Exec(`INSERT INTO catalog_commits (branch_id, commit_id, previous_commit_id,committer, message, creation_date, metadata, merge_type, merge_source_branch, merge_source_commit,
                     lineage_commits)
		VALUES ($1,$2,$3,$4,$5,transaction_timestamp(),$6,relation,$7,$8,string_to_array($9,',')::bigint[])`,
		rightID, nextCommitID, previousMaxCommitID, committer, msg, metadata, leftID, leftLastCommitID, childNewLineage)
	if err != nil {
		return err
	}
	return nil
}

func (c *cataloger) mergeBatchFromChild(tx db.Tx, mergeBatch mergeBatchType, previousMaxCommitID, nextCommitID CommitID, childID int64, parentID int64) error {
	// DifferenceTypeRemoved and DifferenceTypeChanged - set max_commit the our commit for committed entries in parent branch

	// delete(mark max-commit) entries in parent that are either deleted or changed from child
	_, err = tx.Exec(`UPDATE catalog_entries SET max_commit = $2
			WHERE branch_id = $1 AND max_commit = $5
				AND path in (SELECT path FROM `+diffResultsTableName+` WHERE diff_type IN ($3,$4))`,
		parentID, previousMaxCommitID, DifferenceTypeRemoved, DifferenceTypeChanged, MaxCommitID)
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
		VALUES ($1,$2,$3,$4,$5,transaction_timestamp(),$6,'from_child',$7,$8)`,
		parentID, nextCommitID, previousMaxCommitID, committer, msg, metadata, childID, childLastCommitID)
	return err
}

func (c *cataloger) mergeNonDirect(_ context.Context, _ sqlx.Execer, previousMaxCommitID, nextCommitID CommitID, leftID, rightID int64, committer, msg string, _ Metadata) error {
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

func int64Str(l int64) string {
	return strconv.FormatInt(l, 10)
}
