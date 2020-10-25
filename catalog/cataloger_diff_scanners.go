package catalog

import (
	"fmt"

	"context"

	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type diffScanner interface {
	Next() bool
	Value() *diffResultRecord
	Error() error
}

type diffScannerBase struct {
	diffParams                *doDiffParams
	leftScanner, rightScanner *DBLineageScanner
	err                       error
	value                     *diffResultRecord
	diffSummary               map[DifferenceType]int
}

type parentToChildScanner struct {
	diffScannerBase
	childLineage                []lineageCommit
	childLastFromParentCommitID CommitID
	lastChildEnt                *DBScannerEntry
	rowsCounter                 int
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
		differences := make(Differences, 0, params.Limit+1)
		for scanner.Next() {
			v := scanner.Value()
			var d Difference
			d.Entry = v.Entry
			d.Type = v.DiffType
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

func (c *cataloger) newDiffScanner(tx db.Tx, params *doDiffParams) (diffScanner, error) {
	if params == nil {
		return nil, fmt.Errorf("doDiff params are required: %w", ErrInvalidValue)
	}
	relation, err := c.getRefsRelationType(tx, params)
	if err != nil {
		return nil, err
	}
	switch relation {
	case RelationTypeFromParent:
		return c.newDiffFromParent(tx, params)
	//case RelationTypeFromChild:
	//	return c.diffFromChild(ctx, tx, params)
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
}

func (c *cataloger) newDiffFromParent(tx db.Tx, params *doDiffParams) (diffScanner, error) {
	// get child last commit of merge from parent
	scanner := new(parentToChildScanner)
	scanner.diffParams = params
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

func createSummaryMap() map[DifferenceType]int {
	m := make(map[DifferenceType]int)
	for i := 0; i < int(DifferenceTypeNone); i++ {
		m[DifferenceType(i)] = 0
	}
	return m
}

func (c *parentToChildScanner) Next() bool {
	var err error
	if c.diffParams.Limit > -1 && c.rowsCounter >= c.diffParams.Limit {
		return false
	}
	for c.leftScanner.Next() {
		// is parent element is relevant
		parentEnt := c.leftScanner.Value()
		// get next child entry - scan until we match child's path to parent (or bigger)
		c.lastChildEnt, err = ScanDBEntryUntil(c.rightScanner, c.lastChildEnt, parentEnt.Path)
		if err != nil {
			c.err = fmt.Errorf("scan next child element: %w", err)
			return false
		}
		// point to matched child based on path
		var matchedChild *DBScannerEntry
		if c.lastChildEnt != nil && c.lastChildEnt.Path == parentEnt.Path {
			matchedChild = c.lastChildEnt
		}
		// diff between entries
		diffType := evaluateFromParentElementDiffType(c.diffParams.RightBranchID, c.childLastFromParentCommitID, c.childLineage, parentEnt, matchedChild)
		if diffType == DifferenceTypeNone {
			continue
		}
		c.value = &diffResultRecord{DiffType: diffType,
			Entry: parentEnt.Entry,
		}

		// store ctid for copying in the merge step, under the following conditions:
		// 1. the entry exists in the child branch
		// 2. difference type is either changed or added.
		// Then the entry has to appear in the child branch, but an older version exists in the child, so merge will copy it, and the ctid is needed for that
		if matchedChild != nil && matchedChild.BranchID == c.diffParams.RightBranchID && (diffType == DifferenceTypeAdded || diffType == DifferenceTypeChanged) {
			c.value.EntryCtid = &parentEnt.RowCtid
		}
		c.rowsCounter++
		c.diffSummary[diffType]++
		return true // exit for loop and function
	}
	c.err = c.leftScanner.Err()
	return false
}

func (c *parentToChildScanner) Value() *diffResultRecord {
	if c.err == nil {
		return c.value
	} else {
		return nil // will happen only if the caller did not check error
	}
}

func (c *parentToChildScanner) Error() error {
	return c.err
}
