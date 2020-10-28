package catalog

import (
	"fmt"

	"context"
	"errors"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const DiffMaxLimit = 1000

type diffResultRecord struct {
	parentTombstoneNeeded bool
	DiffType              DifferenceType
	Entry                 Entry   // Partially filled. Path is always set.
	EntryCtid             *string // CTID of the modified/added entry. Do not use outside of catalog diff-by-iterators. https://github.com/treeverse/lakeFS/issues/831
}

type diffEffectiveCommits struct {
	// ParentEffectiveCommit last commit parent merged from child.
	// When no sync commit is found - set the commit ID to the point child's branch was created.
	ParentEffectiveCommit CommitID `db:"parent_effective_commit"`

	// ChildEffectiveCommit last commit child merged from parent.
	// If the child never synced with parent, the commit ID is set to 1.
	ChildEffectiveCommit CommitID `db:"child_effective_commit"`

	// ParentEffectiveLineage lineage at the ParentEffectiveCommit
	ParentEffectiveLineage []lineageCommit
}

type doDiffParams struct {
	DiffParams
	Repository    string
	LeftCommitID  CommitID
	LeftBranchID  int64
	RightCommitID CommitID
	RightBranchID int64
}
type diffEvaluator func(c *diffScanner, leftEntry *DBScannerEntry, rightEntry *DBScannerEntry) DifferenceType

type diffScanner struct {
	relation                  RelationType
	diffParams                *doDiffParams
	leftScanner, rightScanner DBScanner
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
		clearChecksumField := !stringIsInSlice(params.AdditionalFields, DBEntryFieldChecksum)
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

		return differences, err
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
	case RelationTypeNotDirect:
		return nil, c.diffNonDirect(tx, params)
	case RelationTypeSame:
		return c.newDiffSameBranch(tx, params)
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
	//scanner.childLineage, err = scanner.rightScanner.ReadLineage()
	scanner.childLineage, err = getLineage(tx, params.RightBranchID, UncommittedID)
	if err != nil {
		return nil, err
	}
	scanner.diffSummary = newSummaryMap()
	return scanner, nil
}

func (c *cataloger) newDiffFromChild(tx db.Tx, params *doDiffParams) (*diffScanner, error) {
	var err error
	scanner := new(diffScanner)
	scanner.diffParams = params
	scanner.diffEvaluator = evaluateChildToParent
	scannerOpts := DBScannerOptions{
		After:            params.After,
		AdditionalFields: prepareDiffAdditionalFields(params.AdditionalFields),
	}
	// get child last commit of merge from parent
	scanner.effectiveCommits, err = c.selectChildEffectiveCommits(tx, params.LeftBranchID, params.RightBranchID)
	if err != nil {
		return nil, err
	}

	scanner.leftScanner = NewDBBranchScanner(tx, params.LeftBranchID, CommittedID, &scannerOpts)
	scanner.rightScanner = NewDBLineageScanner(tx, params.RightBranchID, UncommittedID, &scannerOpts)
	scanner.diffSummary = newSummaryMap()
	return scanner, nil
}

func (c *cataloger) newDiffSameBranch(tx db.Tx, params *doDiffParams) (*diffScanner, error) {
	// get child last commit of merge from parent
	scanner := new(diffScanner)
	scanner.relation = RelationTypeSame
	scanner.diffParams = params
	scanner.diffEvaluator = evaluateSameBranch

	scannerOpts := DBScannerOptions{
		After:            params.After,
		AdditionalFields: prepareDiffAdditionalFields(params.AdditionalFields),
	}
	scanner.leftScanner = NewDBLineageScanner(tx, params.LeftBranchID, params.LeftCommitID, &scannerOpts)
	scanner.rightScanner = NewDBLineageScanner(tx, params.RightBranchID, params.RightCommitID, &scannerOpts)
	scanner.diffSummary = newSummaryMap()
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
		if diffType == DifferenceTypeRemoved &&
			c.relation == RelationTypeFromChild &&
			matchedRight != nil &&
			matchedRight.BranchID != c.rightScanner.getBranchID() {
			c.value.parentTombstoneNeeded = true
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

func evaluateSameBranch(_ *diffScanner, leftEntry *DBScannerEntry, rightEntry *DBScannerEntry) DifferenceType {
	if isNoneDiff(leftEntry, rightEntry) {
		return DifferenceTypeNone
	}
	// source exists and no target - added
	if rightEntry == nil || rightEntry.IsDeleted() {
		return DifferenceTypeAdded
	}
	// assert: right entry is not nil, and not deleted
	if leftEntry.IsDeleted() {
		return DifferenceTypeRemoved
	}
	// If we got to this point
	// 1. both entries exist and are not deleted
	// 2. their checksum do not match (would be caught by is NoneDiff)
	return DifferenceTypeChanged

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

// selectChildEffectiveCommits - read last merge commit numbers from commit table
// if it is the first child-to-parent commit, than those commit numbers are calculated as follows:
// the child is 0, as any change in the child was never merged to the parent.
// the parent is the effective commit number of the first lineage record of the child that points to the parent
// it is possible that the child the have already done from_parent merge. so we have to take the minimal effective commit
func (c *cataloger) selectChildEffectiveCommits(tx db.Tx, childID int64, parentID int64) (*diffEffectiveCommits, error) {
	effectiveCommitsQuery, args, err := psql.Select(`commit_id AS parent_effective_commit`, `merge_source_commit AS child_effective_commit`).
		From("catalog_commits").
		Where("branch_id = ? AND merge_source_branch = ? AND merge_type = 'from_child'", parentID, childID).
		OrderBy(`commit_id DESC`).
		Limit(1).
		ToSql()
	if err != nil {
		return nil, err
	}
	var effectiveCommits diffEffectiveCommits
	err = tx.Get(&effectiveCommits, effectiveCommitsQuery, args...)
	effectiveCommitsNotFound := errors.Is(err, db.ErrNotFound)
	if err != nil && !effectiveCommitsNotFound {
		return nil, err
	}
	if effectiveCommitsNotFound {
		effectiveCommits.ChildEffectiveCommit = 1 // we need all commits from the child. so any small number will do
		parentEffectiveQuery, args, err := psql.Select("commit_id as parent_effective_commit").
			From("catalog_commits").
			Where("branch_id = ? AND merge_source_branch = ? AND merge_type = 'from_parent'", childID, parentID).
			OrderBy("commit_id desc").
			Limit(1).
			ToSql()
		if err != nil {
			return nil, err
		}
		err = tx.GetPrimitive(&effectiveCommits.ParentEffectiveCommit, parentEffectiveQuery, args...)
		if err != nil {
			return nil, err
		}
	}

	effectiveLineage, err := getLineage(tx, parentID, effectiveCommits.ParentEffectiveCommit)
	if err != nil {
		return nil, err
	}
	effectiveCommits.ParentEffectiveLineage = effectiveLineage
	return &effectiveCommits, nil
}

// lineageCommitIDByBranchID lookup the branch ID in lineage and returns the commit ID.
//   If branch ID not found UncommittedID is returned.
func lineageCommitIDByBranchID(lineage []lineageCommit, branchID int64) CommitID {
	for _, l := range lineage {
		if l.BranchID == branchID {
			return l.CommitID
		}
	}
	return UncommittedID
}

//prepareDiffAdditionalFields - make sure we have the required additional fields for diff
func prepareDiffAdditionalFields(fields []string) []string {
	if !stringIsInSlice(fields, DBEntryFieldChecksum) {
		return append(fields, DBEntryFieldChecksum)
	} else {
		return fields
	}
}

func (c *cataloger) getRefsRelationType(tx db.Tx, params *doDiffParams) (RelationType, error) {
	if params.LeftBranchID == params.RightBranchID {
		return RelationTypeSame, nil
	}

	var youngerBranch, olderBranch int64
	var possibleRelation RelationType
	if params.LeftBranchID > params.RightBranchID {
		possibleRelation = RelationTypeFromChild
		youngerBranch = params.LeftBranchID
		olderBranch = params.RightBranchID
	} else {
		possibleRelation = RelationTypeFromParent
		youngerBranch = params.RightBranchID
		olderBranch = params.LeftBranchID
	}

	var isDirectRelation bool
	err := tx.GetPrimitive(&isDirectRelation,
		`select lineage[1]=$1 from catalog_branches where id=$2`, olderBranch, youngerBranch)
	if err != nil {
		return RelationTypeNone, err
	}
	if isDirectRelation {
		return possibleRelation, nil
	}

	return RelationTypeNotDirect, nil
}

func (c *diffEffectiveCommits) ParentEffectiveCommitByBranchID(branchID int64) CommitID {
	for _, l := range c.ParentEffectiveLineage {
		if l.BranchID == branchID {
			return l.CommitID
		}
	}
	return c.ParentEffectiveCommit
}

func stringIsInSlice(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func newSummaryMap() map[DifferenceType]int {
	m := make(map[DifferenceType]int)
	for i := 0; i < int(DifferenceTypeNone); i++ {
		m[DifferenceType(i)] = 0
	}
	return m
}

func (c *cataloger) diffNonDirect(_ db.Tx, params *doDiffParams) error {
	c.log.WithFields(logging.Fields{
		"left_branch_id":  params.LeftBranchID,
		"left_commit_id":  params.LeftCommitID,
		"right_branch_id": params.RightBranchID,
		"right_commit_id": params.RightCommitID,
	}).Debug("Diff not direct - feature not supported")
	return ErrFeatureNotSupported
}
