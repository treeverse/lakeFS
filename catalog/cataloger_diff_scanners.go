package catalog

import (
	"fmt"

	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

//type diffScanner interface {
//	Next() bool
//	Value() *diffResultRecord
//	Error() error
//	evaluateDiffType( leftEntry *DBScannerEntry, rightEntry *DBScannerEntry) DifferenceType
//	getBase() *diffScannerBase
//}

type diffEvaluator func(c *diffScanner, leftEntry *DBScannerEntry, rightEntry *DBScannerEntry) DifferenceType

type diffScanner struct {
	relation                  RelationType
	diffParams                *doDiffParams
	leftScanner, rightScanner *DBLineageScanner
	err                       error
	value                     *diffResultRecord
	lastRightEnt              *DBScannerEntry
	diffSummary               map[DifferenceType]int
	rowsCounter               int
	diffEvaluator             diffEvaluator
	// parent to child vars
	childLineage                []lineageCommit
	childLastFromParentCommitID CommitID
	// child to parent vars
	effectiveCommits *diffEffectiveCommits
}

//
//type parentToChildScanner struct {
//	diffScannerBase
//	childLineage                []lineageCommit
//	childLastFromParentCommitID CommitID
//}
//
//type childToParentScanner struct {
//	diffScannerBase
//	effectiveCommits *diffEffectiveCommits
//}

func (c *cataloger) Diff(ctx context.Context, repository string, leftReference string, rightReference string, params DiffParams) (Differences, bool, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "leftReference", IsValid: ValidateReference(leftReference)},
		{Name: "rightReference", IsValid: ValidateReference(rightReference)},
	}); err != nil {
		return nil, false, err
	}

	// parse references
	leftRef, err := ParseRef(leftReference)
	if err != nil {
		return nil, false, fmt.Errorf("left reference: %w", err)
	}
	rightRef, err := ParseRef(rightReference)
	if err != nil {
		return nil, false, fmt.Errorf("right reference: %w", err)
	}

	if params.Limit < 0 || params.Limit > DiffMaxLimit {
		params.Limit = DiffMaxLimit
	}

	ctx, cancel := c.withDiffResultsContext(ctx)
	defer cancel()

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		// get branch IDs
		leftBranchID, err := c.getBranchIDCache(tx, repository, leftRef.Branch)
		if err != nil {
			return nil, fmt.Errorf("left ref branch: %w", err)
		}
		rightBranchID, err := c.getBranchIDCache(tx, repository, rightRef.Branch)
		if err != nil {
			return nil, fmt.Errorf("right ref branch: %w", err)
		}

		diffParams := &doDiffParams{
			Repository:    repository,
			LeftCommitID:  leftRef.CommitID,
			LeftBranchID:  leftBranchID,
			RightCommitID: rightRef.CommitID,
			RightBranchID: rightBranchID,
			DiffParams: DiffParams{
				// we request additional one (without returning it) for pagination (hasMore)
				Limit:            params.Limit + 1,
				After:            params.After,
				AdditionalFields: params.AdditionalFields,
			},
		}
		scanner, err := c.newDiffScanner(tx, diffParams)
		if err != nil {
			return nil, err
		}
		clearChecksumField := !findString(params.AdditionalFields, DBEntryFieldChecksum)
		differences := make(Differences, 0, params.Limit+1)
		for scanner.Next() {
			v := scanner.Value()
			var d Difference
			d.Entry = v.Entry
			d.Type = v.DiffType
			if clearChecksumField {
				d.Entry.Checksum = ""
			}
			differences = append(differences, d)
		}

		return differences, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, false, err
	}
	differences := res.(Differences)
	hasMore := paginateSlice(&differences, params.Limit)
	return differences, hasMore, nil
}

func (c *cataloger) newDiffScanner(tx db.Tx, params *doDiffParams) (*diffScanner, error) {
	var scanner *diffScanner
	var err error
	if params == nil {
		return nil, fmt.Errorf("doDiff params are required: %w", ErrInvalidValue)
	}
	relation, err := c.getRefsRelationType(tx, params)
	if err != nil {
		return nil, err
	}
	switch relation {
	case RelationTypeFromParent:
		scanner, err = c.newDiffFromParent(tx, params)
	case RelationTypeFromChild:
		scanner, err = c.newDiffFromChild(tx, params)
	//case RelationTypeNotDirect:
	//	return c.diffNonDirect(ctx, tx, params)
	//case RelationTypeSame:
	//	return c.diffSameBranch(ctx, tx, params)
	default:
		c.log.WithFields(logging.Fields{
			"relation_type":   relation,
			"repository":      params.Repository,
			"left_branch_id":  params.LeftBranchID,
			"left_commit_id":  params.LeftCommitID,
			"right_branch_id": params.RightBranchID,
			"right_commit_id": params.RightCommitID,
		}).Debug("Diff by relation - unsupported type")
		return nil, ErrFeatureNotSupported
	}
	scanner.relation = relation
	return scanner, err
}

func (c *cataloger) newDiffFromParent(tx db.Tx, params *doDiffParams) (*diffScanner, error) {
	// get child last commit of merge from parent
	scanner := new(diffScanner)
	scanner.diffParams = params
	scanner.diffEvaluator = evaluateParentToChild
	query, args, err := psql.Select("MAX(commit_id) as max_child_commit").
		From("catalog_commits").
		Where("branch_id = ? AND merge_type = 'from_parent'", params.RightBranchID).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("get child last commit sql: %w", err)
	}
	err = tx.Get(&scanner.childLastFromParentCommitID, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get child last commit failed: %w", err)
	}

	scannerOpts := DBScannerOptions{
		After:            params.After,
		AdditionalFields: prepareDiffAdditionalFields(params.AdditionalFields),
	}
	scanner.leftScanner = NewDBLineageScanner(tx, params.LeftBranchID, CommittedID, &scannerOpts)
	scanner.rightScanner = NewDBLineageScanner(tx, params.RightBranchID, UncommittedID, &scannerOpts)
	scanner.childLineage, err = scanner.rightScanner.ReadLineage()
	if err != nil {
		return nil, err
	}
	scanner.diffSummary = createSummaryMap()
	return scanner, nil
}

func (c *cataloger) newDiffFromChild(tx db.Tx, params *doDiffParams) (*diffScanner, error) {
	// get child last commit of merge from parent
	scanner := new(diffScanner)
	scanner.diffParams = params
	scanner.diffEvaluator = evaluateChildToParent
	scannerOpts := DBScannerOptions{
		After:            params.After,
		AdditionalFields: prepareDiffAdditionalFields(params.AdditionalFields),
	}
	t, err := c.selectChildEffectiveCommits(tx, params.LeftBranchID, params.RightBranchID)
	scanner.effectiveCommits = t // todo: ugly. find right way to do it
	if err != nil {
		return nil, err
	}

	scanner.leftScanner = NewDBLineageScanner(tx, params.LeftBranchID, CommittedID, &scannerOpts)
	scanner.rightScanner = NewDBLineageScanner(tx, params.RightBranchID, UncommittedID, &scannerOpts)
	scanner.diffSummary = createSummaryMap()
	return scanner, nil
}

func (c *diffScanner) Next() bool {
	var err error
	if c.diffParams.Limit > -1 && c.rowsCounter >= c.diffParams.Limit {
		return false
	}
	for c.leftScanner.Next() {
		// is parent element is relevant
		leftEnt := c.leftScanner.Value()
		// get next child entry - scan until we match child's path to parent (or bigger)
		c.lastRightEnt, err = ScanDBEntryUntil(c.rightScanner, c.lastRightEnt, leftEnt.Path)
		if err != nil {
			c.err = fmt.Errorf("scan next right element: %w", err)
			return false
		}
		// point to matched right based on path
		var matchedRight *DBScannerEntry
		if c.lastRightEnt != nil && c.lastRightEnt.Path == leftEnt.Path {
			matchedRight = c.lastRightEnt
		}
		// diff between entries

		diffType := c.diffEvaluator(c, leftEnt, matchedRight)

		if diffType == DifferenceTypeNone {
			continue
		}
		c.value = &diffResultRecord{DiffType: diffType,
			Entry: leftEnt.Entry,
		}

		// store ctid for copying in the merge step, under the following conditions:
		// 1. the entry exists in the child branch
		// 2. difference type is either changed or added.
		// Then the entry has to appear in the child branch, but an older version exists in the child, so merge will copy it, and the ctid is needed for that
		if (diffType == DifferenceTypeAdded || diffType == DifferenceTypeChanged) &&
			((matchedRight != nil && matchedRight.BranchID == c.diffParams.RightBranchID) || c.relation == RelationTypeFromChild) {
			c.value.EntryCtid = &leftEnt.RowCtid
		}

		c.rowsCounter++
		c.diffSummary[diffType]++
		return true // exit for loop and function
	}
	c.err = c.leftScanner.Err()
	return false
}

func (c *diffScanner) Value() *diffResultRecord {
	if c.err == nil {
		return c.value
	} else {
		return nil // will happen only if the caller did not check error
	}
}

func (c *diffScanner) Error() error {
	return c.err
}

func isNoneDiff(leftEntry, rightEntry *DBScannerEntry) bool {
	// both deleted - none
	if leftEntry.IsDeleted() && (rightEntry == nil || rightEntry.IsDeleted()) {
		return true
	}
	// same entry - none
	if rightEntry != nil && leftEntry.IsDeleted() == rightEntry.IsDeleted() && leftEntry.Checksum == rightEntry.Checksum {
		return true
	}
	return false
}
func evaluateParentToChild(c *diffScanner, leftEntry, rightEntry *DBScannerEntry) DifferenceType {
	if isNoneDiff(leftEntry, rightEntry) {
		return DifferenceTypeNone
	}

	// target entry not modified based on commit of the last sync lineage - none
	commitIDByLineage := lineageCommitIDByBranchID(c.childLineage, leftEntry.BranchID)
	parentChangedAfterChild := leftEntry.ChangedAfterCommit(commitIDByLineage)
	if !parentChangedAfterChild {
		return DifferenceTypeNone
	}

	// if target entry is uncommitted - conflict
	// if target entry updated after merge from parent - conflict
	if rightEntry != nil && rightEntry.BranchID == c.diffParams.RightBranchID &&
		(!rightEntry.IsCommitted() || rightEntry.ChangedAfterCommit(c.childLastFromParentCommitID)) {
		return DifferenceTypeConflict
	}

	// if source was deleted (target exists) - removed
	if leftEntry.IsDeleted() {
		return DifferenceTypeRemoved
	}

	// if target deleted (source exists) - add
	if rightEntry == nil || rightEntry.IsDeleted() {
		return DifferenceTypeAdded
	}

	// if target exists - change
	return DifferenceTypeChanged
}

func evaluateChildToParent(c *diffScanner, leftEntry *DBScannerEntry, rightEntry *DBScannerEntry) DifferenceType {
	if isNoneDiff(leftEntry, rightEntry) {
		return DifferenceTypeNone
	}
	if rightEntry != nil {
		// matched target was updated after client - conflict
		effectiveCommitID := c.effectiveCommits.ParentEffectiveCommitByBranchID(rightEntry.BranchID)
		if effectiveCommitID > UncommittedID && rightEntry.MinCommit > effectiveCommitID {
			return DifferenceTypeConflict
		}
	}

	// source deleted - removed
	if leftEntry.IsDeleted() {
		return DifferenceTypeRemoved
	}

	// if target deleted - added
	if rightEntry == nil || rightEntry.IsDeleted() {
		return DifferenceTypeAdded
	}

	// if target found - changed
	return DifferenceTypeChanged

}

func findString(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func createSummaryMap() map[DifferenceType]int {
	m := make(map[DifferenceType]int)
	for i := 0; i < int(DifferenceTypeNone); i++ {
		m[DifferenceType(i)] = 0
	}
	return m
}
