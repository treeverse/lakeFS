package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const (
	DiffMaxLimit = 1000

	diffResultsTableNamePrefix = "catalog_diff_results"
	diffResultsInsertBatchSize = 1024

	contextDiffResultsKey contextKey = "diff_results_key"
)

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

type contextKey string

type diffResultRecord struct {
	SourceBranch int64
	DiffType     DifferenceType
	Entry        Entry   // Partially filled. Path is always set.
	EntryCtid    *string // CTID of the modified/added entry. Do not use outside of catalog diff-by-iterators. https://github.com/treeverse/lakeFS/issues/831
}

type diffResultsBatchWriter struct {
	tx                   db.Tx
	DiffResultsTableName string
	Records              []*diffResultRecord
}

type diffParams struct {
	Repository    string
	LeftCommitID  CommitID
	LeftBranchID  int64
	RightCommitID CommitID
	RightBranchID int64
	Limit         int
	After         string
}

// Diff lists of differences between leftBranch and rightBranch.
// The second return value will be true if there are more results. Use the last entry's path as the next call to Diff in the 'after' argument.
// limit - is the maximum number of differences we will return, limited by DiffMaxLimit (which will be used in case limit less than 0)
// after - lookup entries whose path comes after this value.
// Diff internal API produce temporary table that this call deletes at the end of a successful transaction (failed will rollback changes)
// The diff results table holds ctid to reference the relevant entry for changed/added and source branch for deleted - information used later to apply changes found
func (c *cataloger) Diff(ctx context.Context, repository string, leftReference string, rightReference string, limit int, after string) (Differences, bool, error) {
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

	if limit < 0 || limit > DiffMaxLimit {
		limit = DiffMaxLimit
	}
	// we request additional one (without returning it) for pagination (hasMore)
	diffResultsLimit := limit + 1

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

		params := &diffParams{
			Repository:    repository,
			LeftCommitID:  leftRef.CommitID,
			LeftBranchID:  leftBranchID,
			RightCommitID: rightRef.CommitID,
			RightBranchID: rightBranchID,
			Limit:         diffResultsLimit,
			After:         after,
		}
		err = c.doDiff(ctx, tx, params)
		if err != nil {
			return nil, err
		}
		return getDiffDifferences(ctx, tx, diffResultsLimit, after)
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, false, err
	}
	differences := res.(Differences)
	hasMore := paginateSlice(&differences, limit)
	return differences, hasMore, nil
}

// doDiff internal implementation of the actual diff. limit <0 will scan the complete branch
func (c *cataloger) doDiff(ctx context.Context, tx db.Tx, params *diffParams) error {
	relation, err := c.getRefsRelationType(tx, params)
	if err != nil {
		return err
	}
	switch relation {
	case RelationTypeFromParent:
		return c.diffFromParent(ctx, tx, params)
	case RelationTypeFromChild:
		return c.diffFromChild(ctx, tx, params)
	case RelationTypeNotDirect:
		return c.diffNonDirect(ctx, tx, params)
	case RelationTypeSame:
		return c.diffSameBranch(ctx, tx, params)
	default:
		c.log.WithFields(logging.Fields{
			"relation_type":   relation,
			"repository":      params.Repository,
			"left_branch_id":  params.LeftBranchID,
			"left_commit_id":  params.LeftCommitID,
			"right_branch_id": params.RightBranchID,
			"right_commit_id": params.RightCommitID,
		}).Debug("Diff by relation - unsupported type")
		return ErrFeatureNotSupported
	}
}

func (k contextKey) String() string {
	return string(k)
}

func (c *diffEffectiveCommits) ParentEffectiveCommitByBranchID(branchID int64) CommitID {
	for _, l := range c.ParentEffectiveLineage {
		if l.BranchID == branchID {
			return l.CommitID
		}
	}
	return c.ParentEffectiveCommit
}

func newDiffResultsBatchWriter(tx db.Tx, tableName string) *diffResultsBatchWriter {
	return &diffResultsBatchWriter{
		tx:                   tx,
		DiffResultsTableName: tableName,
		Records:              make([]*diffResultRecord, 0, diffResultsInsertBatchSize),
	}
}

func (d *diffResultsBatchWriter) Write(r *diffResultRecord) error {
	// batch and/or insert results
	d.Records = append(d.Records, r)
	if len(d.Records) < diffResultsInsertBatchSize {
		return nil
	}
	return d.Flush()
}

func (d *diffResultsBatchWriter) Flush() error {
	if len(d.Records) == 0 {
		return nil
	}
	if err := insertDiffResultsBatch(d.tx, d.DiffResultsTableName, d.Records); err != nil {
		return err
	}
	d.Records = d.Records[:0]
	return nil
}

func (c *cataloger) getDiffSummary(ctx context.Context, tx db.Tx) (map[DifferenceType]int, error) {
	var results []struct {
		DiffType int `db:"diff_type"`
		Count    int `db:"count"`
	}
	diffResultsTableName, err := diffResultsTableNameFromContext(ctx)
	if err != nil {
		return nil, err
	}
	err = tx.Select(&results, "SELECT diff_type, count(diff_type) as count FROM "+diffResultsTableName+" GROUP BY diff_type")
	if err != nil {
		return nil, fmt.Errorf("count diff resutls by type: %w", err)
	}
	m := make(map[DifferenceType]int, len(results))
	for _, res := range results {
		m[DifferenceType(res.DiffType)] = res.Count
	}
	return m, nil
}

func (c *cataloger) diffFromParent(ctx context.Context, tx db.Tx, params *diffParams) error {
	// get child last commit of merge from parent
	var childLastFromParentCommitID CommitID
	query, args, err := psql.Select("MAX(commit_id) as max_child_commit").
		From("catalog_commits").
		Where("branch_id = ? AND merge_type = 'from_parent'", params.RightBranchID).
		ToSql()
	if err != nil {
		return fmt.Errorf("get child last commit sql: %w", err)
	}
	err = tx.Get(&childLastFromParentCommitID, query, args...)
	if err != nil {
		return fmt.Errorf("get child last commit failed: %w", err)
	}

	diffResultsTableName, err := createDiffResultsTable(ctx, tx)
	if err != nil {
		return err
	}

	scannerOpts := DBScannerOptions{
		After:            params.After,
		AdditionalFields: []string{DBEntryFieldChecksum},
	}
	parentScanner := NewDBLineageScanner(tx, params.LeftBranchID, CommittedID, &scannerOpts)
	childScanner := NewDBLineageScanner(tx, params.RightBranchID, UncommittedID, &scannerOpts)
	childLineage, err := childScanner.ReadLineage()
	if err != nil {
		return err
	}
	batch := newDiffResultsBatchWriter(tx, diffResultsTableName)
	var childEnt *DBScannerEntry
	records := 0
	for parentScanner.Next() {
		// is parent element is relevant
		parentEnt := parentScanner.Value()

		// get next child entry - scan until we match child's path to parent (or bigger)
		childEnt, err = ScanDBEntryUntil(childScanner, childEnt, parentEnt.Path)
		if err != nil {
			return fmt.Errorf("scan next child element: %w", err)
		}

		// point to matched child based on path
		var matchedChild *DBScannerEntry
		if childEnt != nil && childEnt.Path == parentEnt.Path {
			matchedChild = childEnt
		}
		// diff between entries
		diffType := evaluateFromParentElementDiffType(params.RightBranchID, childLastFromParentCommitID, childLineage, parentEnt, matchedChild)
		if diffType == DifferenceTypeNone {
			continue
		}

		diffRec := &diffResultRecord{
			DiffType: diffType,
			Entry:    parentEnt.Entry,
		}
		if matchedChild != nil && matchedChild.BranchID == params.RightBranchID && diffType != DifferenceTypeConflict && diffType != DifferenceTypeRemoved {
			diffRec.EntryCtid = &parentEnt.RowCtid
		}
		err = batch.Write(diffRec)
		if err != nil {
			return err
		}
		// stop on limit
		records++
		if params.Limit > -1 && records >= params.Limit {
			break
		}
	}
	if err := parentScanner.Err(); err != nil {
		return err
	}
	return batch.Flush()
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

func evaluateFromParentElementDiffType(targetBranchID int64, targetLastSyncCommitID CommitID, targetLastSyncLineage []lineageCommit, sourceEntry *DBScannerEntry, targetEntry *DBScannerEntry) DifferenceType {
	// both deleted - none
	if sourceEntry.IsDeleted() && (targetEntry == nil || targetEntry.IsDeleted()) {
		return DifferenceTypeNone
	}

	// same entry - none
	if targetEntry != nil && sourceEntry.IsDeleted() == targetEntry.IsDeleted() && sourceEntry.Checksum == targetEntry.Checksum {
		return DifferenceTypeNone
	}

	// target entry not modified based on commit of the last sync lineage - none
	commitIDByLineage := lineageCommitIDByBranchID(targetLastSyncLineage, sourceEntry.BranchID)
	parentChangedAfterChild := sourceEntry.ChangedAfterCommit(commitIDByLineage)
	if !parentChangedAfterChild {
		return DifferenceTypeNone
	}

	// if target entry is uncommitted - conflict
	// if target entry updated after merge from parent - conflict
	if targetEntry != nil && targetEntry.BranchID == targetBranchID &&
		(!targetEntry.IsCommitted() || targetEntry.ChangedAfterCommit(targetLastSyncCommitID)) {
		return DifferenceTypeConflict
	}

	// if source was deleted (target exists) - removed
	if sourceEntry.IsDeleted() {
		return DifferenceTypeRemoved
	}

	// if target deleted (source exists) - add
	if targetEntry == nil || targetEntry.IsDeleted() {
		return DifferenceTypeAdded
	}

	// if target exists - change
	return DifferenceTypeChanged
}

func getDiffDifferences(ctx context.Context, tx db.Tx, limit int, after string) (Differences, error) {
	diffResultsTableName, err := diffResultsTableNameFromContext(ctx)
	if err != nil {
		return nil, err
	}
	var result Differences
	query, args, err := psql.Select("diff_type", "path").
		From(diffResultsTableName).
		Where(sq.Gt{"path": after}).
		OrderBy("path").
		Limit(uint64(limit)).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("format diff results query: %w", err)
	}
	err = tx.Select(&result, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select diff results: %w", err)
	}
	return result, nil
}

func (c *cataloger) diffFromChild(ctx context.Context, tx db.Tx, params *diffParams) error {
	effectiveCommits, err := c.selectChildEffectiveCommits(tx, params.LeftBranchID, params.RightBranchID)
	if err != nil {
		return err
	}

	diffResultsTableName, err := createDiffResultsTable(ctx, tx)
	if err != nil {
		return err
	}

	scannerOpts := DBScannerOptions{
		After:            params.After,
		AdditionalFields: []string{DBEntryFieldChecksum},
	}
	childScanner := NewDBBranchScanner(tx, params.LeftBranchID, CommittedID, &scannerOpts)
	parentScanner := NewDBLineageScanner(tx, params.RightBranchID, UncommittedID, &scannerOpts)
	batch := newDiffResultsBatchWriter(tx, diffResultsTableName)
	var parentEnt *DBScannerEntry
	records := 0
	for childScanner.Next() {
		// is child element is relevant
		childEnt := childScanner.Value()
		if !childEnt.ChangedAfterCommit(effectiveCommits.ChildEffectiveCommit) {
			continue
		}

		// get next parent - next parentEnt that path >= child
		parentEnt, err = ScanDBEntryUntil(parentScanner, parentEnt, childEnt.Path)
		if err != nil {
			return fmt.Errorf("scan next parent element: %w", err)
		}

		// point to matched parent entry
		var matchedParent *DBScannerEntry
		if parentEnt != nil && parentEnt.Path == childEnt.Path {
			matchedParent = parentEnt
		}

		diffType := evaluateFromChildElementDiffType(effectiveCommits, childEnt, matchedParent)
		if diffType == DifferenceTypeNone {
			continue
		}

		diffRecord := &diffResultRecord{
			DiffType:  diffType,
			Entry:     childEnt.Entry,
			EntryCtid: &childEnt.RowCtid,
		}
		if diffType == DifferenceTypeRemoved && parentEnt != nil {
			diffRecord.SourceBranch = params.LeftBranchID
		}

		err = batch.Write(diffRecord)
		if err != nil {
			return err
		}

		// stop on limit
		records++
		if params.Limit > -1 && records >= params.Limit {
			break
		}
	}
	if err := childScanner.Err(); err != nil {
		return err
	}
	return batch.Flush()
}

func createDiffResultsTable(ctx context.Context, executor sq.Execer) (string, error) {
	diffResultsTableName, err := diffResultsTableNameFromContext(ctx)
	if err != nil {
		return "", err
	}
	_, err = executor.Exec("CREATE UNLOGGED TABLE " + diffResultsTableName + ` (
		source_branch bigint NOT NULL,
		diff_type integer NOT NULL,
		path character varying COLLATE "C" NOT NULL,
		entry_ctid tid
	)`)
	if err != nil {
		return "", fmt.Errorf("diff result table: %w", err)
	}
	return diffResultsTableName, nil
}

func evaluateFromChildElementDiffType(effectiveCommits *diffEffectiveCommits, childEnt *DBScannerEntry, matchedParent *DBScannerEntry) DifferenceType {
	// both deleted - none
	if childEnt.IsDeleted() && (matchedParent == nil || matchedParent.IsDeleted()) {
		return DifferenceTypeNone
	}

	// same entry - none
	if matchedParent != nil && childEnt.IsDeleted() == matchedParent.IsDeleted() && childEnt.Checksum == matchedParent.Checksum {
		return DifferenceTypeNone
	}

	// both entries are not deleted or point to the same content
	if matchedParent != nil {
		// matched target was updated after client - conflict
		effectiveCommitID := effectiveCommits.ParentEffectiveCommitByBranchID(matchedParent.BranchID)
		if effectiveCommitID > UncommittedID && matchedParent.MinCommit > effectiveCommitID {
			return DifferenceTypeConflict
		}
	}

	// source deleted - removed
	if childEnt.IsDeleted() {
		return DifferenceTypeRemoved
	}

	// if target deleted - added
	if matchedParent == nil || matchedParent.IsDeleted() {
		return DifferenceTypeAdded
	}

	// if target found - changed
	return DifferenceTypeChanged
}

func insertDiffResultsBatch(exerciser sq.Execer, tableName string, batch []*diffResultRecord) error {
	if len(batch) == 0 {
		return nil
	}
	ins := psql.Insert(tableName).Columns("source_branch", "diff_type", "path", "entry_ctid")
	for _, rec := range batch {
		ins = ins.Values(rec.SourceBranch, rec.DiffType, rec.Entry.Path, rec.EntryCtid)
	}
	query, args, err := ins.ToSql()
	if err != nil {
		return fmt.Errorf("query for diff results insert: %w", err)
	}
	_, err = exerciser.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("insert query diff results: %w", err)
	}
	return nil
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
		err = tx.Get(&effectiveCommits.ParentEffectiveCommit, parentEffectiveQuery, args...)
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

// withDiffResultsContext generate diff results id used for temporary table name
func (c *cataloger) withDiffResultsContext(ctx context.Context) (context.Context, context.CancelFunc) {
	id := strings.ReplaceAll(uuid.New().String(), "-", "")
	return context.WithValue(ctx, contextDiffResultsKey, id), func() {
		tableName := diffResultsTableNameFormat(id)
		_, err := c.db.Exec("DROP TABLE IF EXISTS " + tableName)
		if err != nil {
			c.log.WithError(err).WithField("table_name", tableName).Warn("Failed to drop diff results table")
		}
	}
}

func diffResultsTableNameFromContext(ctx context.Context) (string, error) {
	id, ok := ctx.Value(contextDiffResultsKey).(string)
	if !ok {
		return "", ErrMissingDiffResultsIDInContext
	}
	return diffResultsTableNameFormat(id), nil
}

func diffResultsTableNameFormat(id string) string {
	return diffResultsTableNamePrefix + "_" + id
}

func (c *cataloger) diffNonDirect(_ context.Context, _ db.Tx, params *diffParams) error {
	c.log.WithFields(logging.Fields{
		"left_branch_id":  params.LeftBranchID,
		"left_commit_id":  params.LeftCommitID,
		"right_branch_id": params.RightBranchID,
		"right_commit_id": params.RightCommitID,
	}).Debug("Diff not direct - feature not supported")
	return ErrFeatureNotSupported
}

func (c *cataloger) diffSameBranch(ctx context.Context, tx db.Tx, params *diffParams) error {
	diffResultsTableName, err := createDiffResultsTable(ctx, tx)
	if err != nil {
		return err
	}

	scannerOpts := DBScannerOptions{
		After:            params.After,
		AdditionalFields: []string{DBEntryFieldChecksum},
	}
	sourceScanner := NewDBBranchScanner(tx, params.LeftBranchID, params.LeftCommitID, &scannerOpts)
	targetScanner := NewDBLineageScanner(tx, params.RightBranchID, params.RightCommitID, &scannerOpts)
	batch := newDiffResultsBatchWriter(tx, diffResultsTableName)
	var targetNextEnt *DBScannerEntry
	records := 0
	for sourceScanner.Next() {
		sourceEnt := sourceScanner.Value()
		targetNextEnt, err = ScanDBEntryUntil(targetScanner, targetNextEnt, sourceEnt.Path)
		if err != nil {
			return fmt.Errorf("scan next parent element: %w", err)
		}

		// point to matched child based on path
		var targetEnt *DBScannerEntry
		if targetNextEnt != nil && sourceEnt.Path == targetNextEnt.Path {
			targetEnt = targetNextEnt
		}

		diffType := evaluateSameBranchElementDiffType(sourceEnt, targetEnt)
		if diffType == DifferenceTypeNone {
			continue
		}

		err = batch.Write(&diffResultRecord{
			DiffType: diffType,
			Entry:    sourceEnt.Entry,
		})
		if err != nil {
			return err
		}

		// stop on limit
		records++
		if params.Limit > -1 && records >= params.Limit {
			break
		}
	}
	if err := sourceScanner.Err(); err != nil {
		return err
	}
	return batch.Flush()
}

func (c *cataloger) getRefsRelationType(tx db.Tx, params *diffParams) (RelationType, error) {
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
	err := tx.Get(&isDirectRelation,
		`select lineage[1]=$1 from catalog_branches where id=$2`, olderBranch, youngerBranch)
	if err != nil {
		return RelationTypeNone, err
	}
	if isDirectRelation {
		return possibleRelation, nil
	}

	return RelationTypeNotDirect, nil
}

func evaluateSameBranchElementDiffType(sourceEnt *DBScannerEntry, targetEnt *DBScannerEntry) DifferenceType {
	if sourceEnt.IsDeleted() {
		// both deleted - none
		if targetEnt == nil || targetEnt.IsDeleted() {
			return DifferenceTypeNone
		}
		// source exists, target not - removed
		return DifferenceTypeRemoved
	}
	// source exists and no target - added
	if targetEnt == nil || targetEnt.IsDeleted() {
		return DifferenceTypeAdded
	}
	// source and target not matched - change
	if targetEnt.BranchID == sourceEnt.BranchID && targetEnt.MinCommit != sourceEnt.MinCommit {
		return DifferenceTypeChanged
	}
	// entries match - none
	return DifferenceTypeNone
}
