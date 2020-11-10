package catalog

import (
	"errors"
	"fmt"

	"github.com/treeverse/lakefs/db"
)

type doDiffParams struct {
	DiffParams
	Repository    string
	LeftCommitID  CommitID
	LeftBranchID  int64
	RightCommitID CommitID
	RightBranchID int64
}

type diffEvaluator func(leftEntry *DBScannerEntry, rightEntry *DBScannerEntry) DifferenceType

type DiffScanner struct {
	Relation                    RelationType
	params                      doDiffParams
	leftScanner                 DBScanner
	rightScanner                DBScanner
	err                         error
	value                       *DiffResultRecord
	evaluator                   diffEvaluator
	childLineage                []lineageCommit       // used by diff from parent to child
	childLastFromParentCommitID CommitID              // used by diff from parent to child
	effectiveCommits            *diffEffectiveCommits // used by diff from child to parent
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

func NewDiffScanner(tx db.Tx, params doDiffParams) (*DiffScanner, error) {
	relation, err := getRefsRelationType(tx, params)
	if err != nil {
		return nil, err
	}
	if relation == RelationTypeNotDirect {
		return nil, ErrNonDirectNotSupported
	}
	scanner := &DiffScanner{
		Relation: relation,
		params:   params,
	}
	scannerOpts := DBLineageScannerOptions{
		DBScannerOptions: DBScannerOptions{
			After:            params.After,
			AdditionalFields: prepareDiffAdditionalFields(params.AdditionalFields),
		},
	}
	switch relation {
	case RelationTypeFromParent:
		return scanner.diffFromParent(tx, params, scannerOpts)
	case RelationTypeFromChild:
		return scanner.diffFromChild(tx, params, scannerOpts)
	case RelationTypeSame:
		return scanner.diffSameBranch(tx, params, scannerOpts)
	default:
		return nil, ErrFeatureNotSupported
	}
}

func (s *DiffScanner) diffFromParent(tx db.Tx, params doDiffParams, scannerOpts DBLineageScannerOptions) (*DiffScanner, error) {
	// get child last commit of merge from parent
	s.evaluator = s.evaluateParentToChild
	query, args, err := psql.Select("MAX(commit_id) as max_child_commit").
		From("catalog_commits").
		Where("branch_id = ? AND merge_type = 'from_parent'", params.RightBranchID).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("get child last commit sql: %w", err)
	}
	err = tx.Get(&s.childLastFromParentCommitID, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get child last commit failed: %w", err)
	}
	leftLineage, err := getLineage(tx, params.LeftBranchID, CommittedID)
	if err != nil {
		return nil, fmt.Errorf("get left branch lineage: %w", err)
	}
	rightLineage, err := getLineage(tx, params.RightBranchID, UncommittedID)
	if err != nil {
		return nil, fmt.Errorf("get right branch lineage: %w", err)
	}
	// If some ancestor branch commit id is the same for parent and child - then the parent does not need to read it
	// so it is trimmed from the parent lineage
	if len(rightLineage)-len(leftLineage) != 1 {
		return nil, fmt.Errorf("corrupted lineage definitions:%w", ErrLineageCorrupted)
	}
	for i := range leftLineage {
		if leftLineage[i].CommitID == rightLineage[i+1].CommitID {
			leftLineage = leftLineage[:i]
			break
		}
	}

	scannerOpts.Lineage = leftLineage
	s.leftScanner = NewDBLineageScanner(tx, params.LeftBranchID, CommittedID, scannerOpts)
	scannerOpts.Lineage = rightLineage
	s.rightScanner = NewDBLineageScanner(tx, params.RightBranchID, UncommittedID, scannerOpts)
	s.childLineage = rightLineage
	return s, nil
}

func (s *DiffScanner) diffFromChild(tx db.Tx, params doDiffParams, scannerOpts DBLineageScannerOptions) (*DiffScanner, error) {
	var err error
	s.evaluator = s.evaluateChildToParent
	// get child last commit of merge from parent
	s.effectiveCommits, err = selectChildEffectiveCommits(tx, params.LeftBranchID, params.RightBranchID)
	if err != nil {
		return nil, err
	}
	s.leftScanner = NewDBBranchScanner(tx, params.LeftBranchID, CommittedID, scannerOpts.DBScannerOptions)
	s.rightScanner = NewDBLineageScanner(tx, params.RightBranchID, UncommittedID, scannerOpts)
	return s, nil
}

func (s *DiffScanner) diffSameBranch(tx db.Tx, params doDiffParams, scannerOpts DBLineageScannerOptions) (*DiffScanner, error) {
	// get child last commit of merge from parent
	s.evaluator = evaluateSameBranch
	s.leftScanner = NewDBLineageScanner(tx, params.LeftBranchID, params.LeftCommitID, scannerOpts)
	s.rightScanner = NewDBLineageScanner(tx, params.RightBranchID, params.RightCommitID, scannerOpts)
	return s, nil
}

func (s *DiffScanner) Next() bool {
	for s.leftScanner.Next() {
		leftEnt := s.leftScanner.Value()
		// get next right entry - scan until we match right path to left (or bigger)
		err := ScanDBEntriesUntil(s.rightScanner, leftEnt.Path)
		if err != nil {
			s.err = fmt.Errorf("scan next right element: %w", err)
			return false
		}
		// point to matched right based on path
		var matchedRight *DBScannerEntry
		rightEnt := s.rightScanner.Value()
		if rightEnt != nil && rightEnt.Path == leftEnt.Path {
			matchedRight = rightEnt
		}
		diffType := s.evaluator(leftEnt, matchedRight)
		if diffType == DifferenceTypeNone {
			continue
		}
		s.value = &DiffResultRecord{
			Difference: Difference{
				Type:  diffType,
				Entry: leftEnt.Entry,
			},
		}

		// store ctid for copying in the merge step, under the following conditions:
		// 1. the entry exists in the child branch
		// 2. difference type is either changed or added.
		// Then the entry has to appear in the child branch, but an older version exists in the child, so merge will copy it, and the ctid is needed for that
		if (diffType == DifferenceTypeAdded || diffType == DifferenceTypeChanged) &&
			((matchedRight != nil && matchedRight.BranchID == s.params.RightBranchID) || s.Relation == RelationTypeFromChild) {
			s.value.EntryCtid = &leftEnt.RowCtid
		}
		if diffType == DifferenceTypeRemoved &&
			s.Relation == RelationTypeFromChild &&
			matchedRight != nil &&
			matchedRight.BranchID != s.params.RightBranchID {
			s.value.TargetEntryNotInDirectBranch = true
		}
		return true // exit for loop and function
	}
	s.err = s.leftScanner.Err()
	return false
}

func (s *DiffScanner) Value() *DiffResultRecord {
	if s.err != nil {
		return nil
	}
	return s.value
}

func (s *DiffScanner) Error() error {
	return s.err
}

func (s *DiffScanner) evaluateParentToChild(leftEntry, rightEntry *DBScannerEntry) DifferenceType {
	if isNoneDiff(leftEntry, rightEntry) {
		return DifferenceTypeNone
	}

	// target entry not modified based on commit of the last sync lineage - none
	commitIDByLineage := lineageCommitIDByBranchID(s.childLineage, leftEntry.BranchID)
	parentChangedAfterChild := leftEntry.ChangedAfterCommit(commitIDByLineage)
	if !parentChangedAfterChild {
		return DifferenceTypeNone
	}

	// if target entry is uncommitted - conflict
	// if target entry updated after merge from parent - conflict
	if rightEntry != nil && rightEntry.BranchID == s.params.RightBranchID &&
		(!rightEntry.IsCommitted() || rightEntry.ChangedAfterCommit(s.childLastFromParentCommitID)) {
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

func (s *DiffScanner) evaluateChildToParent(leftEntry *DBScannerEntry, rightEntry *DBScannerEntry) DifferenceType {
	if isNoneDiff(leftEntry, rightEntry) {
		return DifferenceTypeNone
	}
	if rightEntry != nil {
		// matched target was updated after client - conflict
		effectiveCommitID := s.effectiveCommits.ParentEffectiveCommitByBranchID(rightEntry.BranchID)
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

func evaluateSameBranch(leftEntry *DBScannerEntry, rightEntry *DBScannerEntry) DifferenceType {
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
func selectChildEffectiveCommits(tx db.Tx, childID int64, parentID int64) (*diffEffectiveCommits, error) {
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

// prepareDiffAdditionalFields - make sure we have checksum field for diff
func prepareDiffAdditionalFields(fields []string) []string {
	for _, item := range fields {
		if item == DBEntryFieldChecksum {
			return fields
		}
	}
	return append(fields, DBEntryFieldChecksum)
}

func getRefsRelationType(tx db.Tx, params doDiffParams) (RelationType, error) {
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
