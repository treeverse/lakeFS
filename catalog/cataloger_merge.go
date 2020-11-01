package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

const (
	MergeBatchSize       = 256
	MergeBatchChanBuffer = 10
)

type mergeBatchRecords struct {
	err         error
	differences []*diffResultRecord
}

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
	mergeResult := &MergeResult{Summary: make(map[DifferenceType]int)}
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
			DiffParams: DiffParams{
				Limit: -1,
			},
		}
		relation, err := getRefsRelationType(tx, params)
		if err != nil {
			return nil, err
		}
		if relation == RelationTypeSame {
			return nil, ErrSameBranchMergeNotSupported
		}
		nextCommitID, err := getNextCommitID(tx)
		if err != nil {
			return nil, err
		}
		previousMaxCommitID, err := getLastCommitIDByBranchID(tx, rightID)
		if err != nil {
			return nil, err
		}
		mergeBatchChan := make(chan mergeBatchRecords, MergeBatchChanBuffer)
		var workerExitFlag bool
		go c.diffWorker(params, mergeBatchChan, &workerExitFlag)
		var rowsCounter int
		for buf := range mergeBatchChan {
			if buf.err != nil {
				workerExitFlag = true
				drainChannel(mergeBatchChan)
				return nil, buf.err
			}
			for _, d := range buf.differences {
				if d.Type == DifferenceTypeConflict {
					workerExitFlag = true
					drainChannel(mergeBatchChan)
					return nil, ErrConflictFound
				}
				mergeResult.Summary[d.Type]++
				rowsCounter++
			}
			err = applyDiffChangesToRightBranch(tx, buf, previousMaxCommitID, nextCommitID, rightID, relation)
			if err != nil {
				workerExitFlag = true
				drainChannel(mergeBatchChan)
				return nil, err
			}
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
				return nil, ErrNoDifferenceWasFound
			}
		}
		err = insertMergeCommit(tx, relation, leftID, rightID, nextCommitID, previousMaxCommitID, committer, message, metadata)
		if err != nil {
			return nil, err
		}
		mergeResult.Reference = MakeReference(rightBranch, nextCommitID)
		for _, hook := range c.hooks.PostMerge {
			err = hook(ctx, tx, mergeResult)
			if err != nil {
				// Roll tx back if a hook failed
				return nil, err
			}
		}

		return nil, nil
	}, c.txOpts(ctx)...)
	return mergeResult, err
}

func drainChannel(channel chan mergeBatchRecords) {
	// If merge just exited on error, than the diffWorker could continue sending to channel until the buffer
	// got full, and diffWorker would stay in memory, in the middle of a transaction - something that
	// may create problems after a while.
	// channel draining is needed so that diffWorker does not get stuck, and  can exit the transaction
	for ok := true; ok; { // drain the channel, till it closes
		_, ok = <-channel
	}
}

func (c *cataloger) diffWorker(params doDiffParams, mergeBatchChan chan mergeBatchRecords, exitFlag *bool) {
	_, _ = c.db.Transact(func(tx db.Tx) (interface{}, error) {
		defer close(mergeBatchChan)
		mergeBatch := mergeBatchRecords{differences: make([]*diffResultRecord, 0, MergeBatchSize)}
		scanner, err := NewDiffScanner(tx, params)
		if err != nil {
			mergeBatch.err = err
			mergeBatchChan <- mergeBatch
			return nil, nil
		}
		for scanner.Next() {
			if *exitFlag {
				return nil, nil
			}
			v := scanner.Value()
			mergeBatch.differences = append(mergeBatch.differences, v)
			if len(mergeBatch.differences) >= MergeBatchSize {
				mergeBatchChan <- mergeBatch
				mergeBatch.differences = make([]*diffResultRecord, 0, MergeBatchSize)
			}
		}
		mergeBatch.err = scanner.Error()
		mergeBatchChan <- mergeBatch
		return nil, nil
	}, db.ReadOnly())
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
	for _, diffRec := range mergeBatch.differences {
		if diffRec.Type == DifferenceTypeRemoved || diffRec.Type == DifferenceTypeChanged {
			paths = append(paths, diffRec.Entry.Path)
		}
		if (diffRec.Type == DifferenceTypeAdded || diffRec.Type == DifferenceTypeChanged) &&
			diffRec.EntryCtid != nil {
			ctidArray = append(ctidArray, *diffRec.EntryCtid)
		}
		if diffRec.Type == DifferenceTypeRemoved &&
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
		values := "(VALUES ('" + strings.Join(tombstonePaths, "'),('") + "')) AS t(path)"
		sql := `INSERT INTO catalog_entries (branch_id,path,physical_address,size,checksum,metadata,min_commit,max_commit)
				SELECT $1,path,'',0,'','{}',$2,0 FROM ` + values
		_, err := tx.Exec(sql, rightID, previousMaxCommitID)
		if err != nil {
			return err
		}
	}
	return nil
}
func insertMergeCommit(tx db.Tx, relation RelationType, leftID int64, rightID int64, nextCommitID CommitID, previousMaxCommitID CommitID, committer string, msg string, metadata Metadata) error {
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
