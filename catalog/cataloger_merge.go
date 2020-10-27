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

		scanner, err := c.newDiffScanner(tx, params)
		if err != nil {
			return nil, err
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
		if relation == RelationTypeNotDirect {
			err = c.mergeNonDirect(ctx, tx, previousMaxCommitID, nextCommitID, leftID, rightID, committer, message, metadata)
			return nil, err
		}
		differences := make(mergeBatchType, 0, MergeBatchSize)
		for scanner.Next() {
			v := scanner.Value()
			if v.DiffType == DifferenceTypeConflict {
				return nil, ErrConflictFound
			}
			differences = append(differences, v)
			if len(differences) >= MergeBatchSize {
				err = mergeBatch(tx, differences, previousMaxCommitID, nextCommitID, leftID, rightID)
				if err != nil {
					return nil, err
				}
				differences = differences[:0]
			}
		}
		err = scanner.Error()
		if err != nil {
			return nil, err
		}
		err = mergeBatch(tx, differences, previousMaxCommitID, nextCommitID, leftID, rightID)
		if err != nil {
			return nil, err
		}
		if message == "" {
			message = fmt.Sprintf("Merge '%s' into '%s'", leftBranch, rightBranch)
		}
		err = InsertMergeCommit(tx, relation, leftID, rightID, nextCommitID, previousMaxCommitID, committer, message, metadata)
		mergeResult.Summary = scanner.diffSummary
		mergeResult.Reference = MakeReference(rightBranch, nextCommitID)
		return mergeResult, err
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

func mergeBatch(tx db.Tx, mergeBatch mergeBatchType, previousMaxCommitID, nextCommitID CommitID, parentID, childID int64) error {
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
			Set("max_commit", previousMaxCommitID).
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
		t := int64Str(int64(leftLastCommitID))
		childNewLineage = &t
		if len(parentLastLineage) > 0 {
			*childNewLineage += "," + parentLastLineage
		}
	}
	_, err = tx.Exec(`INSERT INTO catalog_commits (branch_id, commit_id, previous_commit_id,committer, message, creation_date, metadata, merge_type, merge_source_branch, merge_source_commit,
                     lineage_commits)
		VALUES ($1,$2,$3,$4,$5,transaction_timestamp(),$6,$10,$7,$8,string_to_array($9,',')::bigint[])`,
		rightID, nextCommitID, previousMaxCommitID, committer, msg, metadata, leftID, leftLastCommitID, childNewLineage, relation)
	if err != nil {
		return err
	}
	return nil
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
