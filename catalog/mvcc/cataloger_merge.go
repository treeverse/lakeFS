package mvcc

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
)

const (
	MergeBatchSize       = 256
	MergeBatchChanBuffer = 10
)

type mergeBatchRecords []*catalog.DiffResultRecord

// Merge perform diff between two branches (left and right), apply changes on right branch and commit
// It uses the cataloger diff internal API to produce a temporary table that we delete at the end of a successful merge
// the table holds entry ctid to reference entries in case of changed/added and source branch in case of delete.
// That information is used to address cases where we need to create new entry or tombstone as part of the merge
func (c *cataloger) Merge(ctx context.Context, repository, leftBranch, rightBranch, committer, message string, metadata catalog.Metadata) (*catalog.MergeResult, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "leftBranch", IsValid: ValidateBranchName(leftBranch)},
		{Name: "rightBranch", IsValid: ValidateBranchName(rightBranch)},
		{Name: "committer", IsValid: ValidateCommitter(committer)},
	}); err != nil {
		return nil, err
	}

	mergeResult := &catalog.MergeResult{
		Summary: make(map[catalog.DifferenceType]int),
	}
	_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		leftID, err := getBranchID(tx, repository, leftBranch, LockTypeUpdate)
		if err != nil {
			return nil, fmt.Errorf("left branch: %w", err)
		}
		rightID, err := getBranchID(tx, repository, rightBranch, LockTypeUpdate)
		if err != nil {
			return nil, fmt.Errorf("right branch: %w", err)
		}
		params := doDiffParams{
			Repository:    repository,
			LeftCommitID:  CommittedID,
			LeftBranchID:  leftID,
			RightCommitID: UncommittedID,
			RightBranchID: rightID,
			DiffParams: catalog.DiffParams{
				Limit: -1,
			},
		}
		relation, err := getRefsRelationType(tx, params)
		if err != nil {
			return nil, err
		}
		if relation == RelationTypeSame {
			return nil, catalog.ErrSameBranchMergeNotSupported
		}
		nextCommitID, err := getNextCommitID(tx)
		if err != nil {
			return nil, err
		}
		previousMaxCommitID, err := getLastCommitIDByBranchID(tx, rightID)
		if err != nil {
			return nil, err
		}
		rowsCounter, err := c.doMerge(ctx, tx, params, mergeResult, previousMaxCommitID, nextCommitID, relation)
		if err != nil {
			return nil, err
		}
		if message == "" {
			message = fmt.Sprintf("Merge '%s' into '%s'", leftBranch, rightBranch)
		}
		if rowsCounter == 0 {
			commitDifferences, err := hasCommitDifferences(tx, leftID, rightID)
			if err != nil {
				return nil, err
			}
			if !commitDifferences {
				return nil, catalog.ErrNoDifferenceWasFound
			}
		}
		err = insertMergeCommit(tx, relation, leftID, rightID, nextCommitID, previousMaxCommitID, committer, message, metadata)
		if err != nil {
			return nil, err
		}
		mergeResult.Reference = MakeReference(rightBranch, nextCommitID)
		return nil, nil
	}, c.txOpts(ctx, db.ReadCommitted())...)

	if err == nil {
		for _, hook := range c.Hooks().PostMerge {
			anotherErr := hook(ctx, repository, rightBranch, mergeResult)
			if anotherErr != nil && err == nil {
				err = anotherErr
			}
		}
	}

	return mergeResult, err
}

func (c *cataloger) doMerge(ctx context.Context, tx db.Tx, params doDiffParams, mergeResult *catalog.MergeResult, previousMaxCommitID CommitID, nextCommitID CommitID, relation RelationType) (int, error) {
	mergeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	mergeBatchChan, errChan := c.initDiffWorker(mergeCtx, params)
	var rowsCounter int
	for {
		select {
		case buf, ok := <-mergeBatchChan:
			if !ok {
				return rowsCounter, nil
			} else {
				for _, d := range buf {
					mergeResult.Summary[d.Type]++
					rowsCounter++
					if d.Type == catalog.DifferenceTypeConflict {
						return rowsCounter, catalog.ErrConflictFound
					}
				}
				err := applyDiffChangesToRightBranch(tx, buf, previousMaxCommitID, nextCommitID, params.RightBranchID, relation)
				if err != nil {
					return rowsCounter, err
				}
			}
		case err := <-errChan:
			return rowsCounter, err
		}
	}
}

func (c *cataloger) initDiffWorker(ctx context.Context, params doDiffParams) (chan mergeBatchRecords, chan error) {
	mergeBatchChan := make(chan mergeBatchRecords, MergeBatchChanBuffer)
	errChan := make(chan error, 1)
	go func() {
		defer close(errChan)
		defer close(mergeBatchChan)
		_, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
			mergeBatch := make([]*catalog.DiffResultRecord, 0, MergeBatchSize)
			scanner, err := NewDiffScanner(tx, params)
			if err != nil {
				return nil, err
			}
			for scanner.Next() {
				v := scanner.Value()
				mergeBatch = append(mergeBatch, v)
				if len(mergeBatch) >= MergeBatchSize {
					select {
					case mergeBatchChan <- mergeBatch:
						mergeBatch = make([]*catalog.DiffResultRecord, 0, MergeBatchSize)
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
			}
			if scanner.Error() != nil {
				return nil, scanner.Error()
			}
			if len(mergeBatch) > 0 {
				select {
				case mergeBatchChan <- mergeBatch:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		}, c.txOpts(ctx, db.ReadOnly())...)
		if err != nil {
			errChan <- err
		}
	}()
	return mergeBatchChan, errChan
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
	err := tx.GetPrimitive(&hasCommitDifferences, mergeCommitsQuery, rightID, leftID)
	if errors.Is(err, db.ErrNotFound) {
		// not found errors indicate there is no merge record for this relation
		//  a parent to child merge record is written when the branch is created,
		// so this may happen only on first child to parent merge.
		// in this case - a check is done if any commits where done to child.
		const checkChildCommitsQuery = "select exists(select * from catalog_commits where branch_id = $1 and merge_type='none')"
		err = tx.GetPrimitive(&hasCommitDifferences, checkChildCommitsQuery, leftID)
	}
	if err != nil {
		return false, fmt.Errorf("has commit difference: %w", err)
	}
	return hasCommitDifferences, nil
}

func applyDiffChangesToRightBranch(tx db.Tx, mergeBatch mergeBatchRecords, previousMaxCommitID, nextCommitID CommitID, rightID int64, relation RelationType) error {
	// collect changes to apply  on the branch
	paths := make([]string, 0, MergeBatchSize)
	ctidArray := make([]string, 0, MergeBatchSize)
	var tombstonePaths []string
	for _, diffRec := range mergeBatch {
		if diffRec.Type == catalog.DifferenceTypeRemoved || diffRec.Type == catalog.DifferenceTypeChanged {
			paths = append(paths, diffRec.Entry.Path)
		}
		if (diffRec.Type == catalog.DifferenceTypeAdded || diffRec.Type == catalog.DifferenceTypeChanged) &&
			diffRec.EntryCtid != nil {
			ctidArray = append(ctidArray, *diffRec.EntryCtid)
		}
		if diffRec.Type == catalog.DifferenceTypeRemoved &&
			diffRec.TargetEntryNotInDirectBranch &&
			relation == RelationTypeFromChild {
			tombstonePaths = append(tombstonePaths, diffRec.Entry.Path)
		}
	}
	// apply changes
	if len(paths) > 0 {
		// set entries that exist in the right branch as deleted by entries that were removed or changed
		setMaxCommit := sq.Update("catalog_entries").
			Set("max_commit", previousMaxCommitID).
			Where("branch_id = ? and max_commit = ?", rightID, MaxCommitID).
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
		// copy entries from left to right
		internalSelect := sq.Select().
			Column("?", rightID).
			Columns("path", "physical_address", "creation_date", "size", "checksum", "metadata").
			Column("?", nextCommitID).
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
	// insert tombstones into parent branch that has a removed entry in its lineage
	if len(tombstonePaths) > 0 {
		sql := `INSERT INTO catalog_entries (branch_id,path,physical_address,size,checksum,metadata,min_commit,max_commit)
				SELECT $1,path,'',0,'','{}',$2,0 FROM UNNEST($3::text []) path`
		_, err := tx.Exec(sql, rightID, previousMaxCommitID, tombstonePaths)
		if err != nil {
			return err
		}
	}
	return nil
}
func insertMergeCommit(tx db.Tx, relation RelationType, leftID int64, rightID int64, nextCommitID CommitID, previousMaxCommitID CommitID, committer string, msg string, metadata catalog.Metadata) error {
	var childNewLineage []int64
	leftLastCommitID, err := getLastCommitIDByBranchID(tx, leftID)
	if err != nil {
		return err
	}
	if relation == RelationTypeFromParent {
		var parentLastLineage []int64
		err = tx.Get(&parentLastLineage, `SELECT DISTINCT ON (branch_id) lineage_commits FROM catalog_commits
												  WHERE branch_id = $1 AND merge_type = 'from_parent' ORDER BY branch_id,commit_id DESC`, leftID)
		if err != nil && !errors.As(err, &db.ErrNotFound) {
			return err
		}
		childNewLineage = append([]int64{int64(leftLastCommitID)}, parentLastLineage...)
	}
	_, err = tx.Exec(`INSERT INTO catalog_commits (branch_id, commit_id, previous_commit_id,committer, message, creation_date, metadata, merge_source_branch, merge_source_commit,
                     lineage_commits, merge_type)
		VALUES ($1,$2,$3,$4,$5,transaction_timestamp(),$6,$7,$8,$9,$10)`,
		rightID, nextCommitID, previousMaxCommitID, committer, msg, metadata, leftID, leftLastCommitID, childNewLineage, relation)
	return err
}
