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
	// ParentEffectiveCommit last commit parent synchronized with child.
	// When no sync commit is found - set the commit ID to the point child's branch was created.
	ParentEffectiveCommit CommitID `db:"parent_effective_commit"`

	// ChildEffectiveCommit last commit child synchronized to parent.
	// If the child never synced with parent, the commit ID is set to 1.
	ChildEffectiveCommit CommitID `db:"child_effective_commit"`

	// ParentEffectiveLineage lineage at the ParentEffectiveCommit
	ParentEffectiveLineage []lineageCommit
}

type contextKey string

type diffResultRecord struct {
	SourceBranch int64
	DiffType     DifferenceType
	Entry        Entry
	EntryCtid    *string
}

type diffResultsBatchWriter struct {
	tx                   db.Tx
	DiffResultsTableName string
	Records              []*diffResultRecord
}

var ErrMissingDiffResultsIDInContext = errors.New("missing diff results id in context")

func (c *cataloger) Diff(ctx context.Context, repository string, leftBranch string, rightBranch string, limit int, after string) (Differences, bool, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "leftBranch", IsValid: ValidateBranchName(leftBranch)},
		{Name: "rightBranch", IsValid: ValidateBranchName(rightBranch)},
	}); err != nil {
		return nil, false, err
	}

	if limit < 0 || limit > DiffMaxLimit {
		limit = DiffMaxLimit
	}
	// we request additional one (without returning it) for pagination (hasMore)
	diffResultsLimit := limit + 1

	ctx, cancel := c.withDiffResultsContext(ctx)
	defer cancel()

	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		leftID, err := c.getBranchIDCache(tx, repository, leftBranch)
		if err != nil {
			return nil, fmt.Errorf("left branch: %w", err)
		}
		rightID, err := c.getBranchIDCache(tx, repository, rightBranch)
		if err != nil {
			return nil, fmt.Errorf("right branch: %w", err)
		}
		err = c.doDiff(ctx, tx, leftID, rightID, diffResultsLimit, after)
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

func (c *cataloger) doDiff(ctx context.Context, tx db.Tx, leftID, rightID int64, limit int, after string) error {
	relation, err := getBranchesRelationType(tx, leftID, rightID)
	if err != nil {
		return err
	}
	return c.doDiffByRelation(ctx, tx, relation, leftID, rightID, limit, after)
}

// doDiffByRelation underlying diff between two branches, called by diff and merge
func (c *cataloger) doDiffByRelation(ctx context.Context, tx db.Tx, relation RelationType, leftID, rightID int64, limit int, after string) error {
	switch relation {
	case RelationTypeFromParent:
		return c.diffFromParent(ctx, tx, leftID, rightID, limit, after)
	case RelationTypeFromChild:
		return c.diffFromChild(ctx, tx, leftID, rightID, limit, after)
	case RelationTypeNotDirect:
		return c.diffNonDirect(ctx, tx, leftID, rightID, limit, after)
	default:
		c.log.WithFields(logging.Fields{
			"relation_type": relation,
			"left_id":       leftID,
			"right_id":      rightID,
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

func (c *cataloger) diffFromParent(ctx context.Context, tx db.Tx, parentID, childID int64, limit int, after string) error {
	// get child last commit of merge from parent
	var childLastFromParentCommitID CommitID
	query, args, err := sq.Select("MAX(commit_id) as max_child_commit").
		From("catalog_commits").
		Where("branch_id = ? AND merge_type = 'from_parent'", childID).
		PlaceholderFormat(sq.Dollar).
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
		After:            after,
		AdditionalFields: []string{DBEntryFieldChecksum},
	}
	parentScanner := NewDBLineageScanner(tx, parentID, CommittedID, &scannerOpts)
	childScanner := NewDBLineageScanner(tx, childID, UncommittedID, &scannerOpts)
	childLineage, err := childScanner.ReadLineage()
	if err != nil {
		return err
	}
	batch := newDiffResultsBatchWriter(tx, diffResultsTableName)
	var childEnt *DBScannerEntry
	records := 0
	for parentScanner.Next() {
		// stop on limit
		if limit > -1 && records >= limit {
			break
		}

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
		diffType := evaluateFromParentElementDiffType(childID, childLastFromParentCommitID, childLineage, parentEnt, matchedChild)
		if diffType == DifferenceTypeNone {
			continue
		}

		diffRec := &diffResultRecord{
			SourceBranch: parentID,
			DiffType:     diffType,
			Entry:        parentEnt.Entry,
		}
		if matchedChild != nil && matchedChild.BranchID == childID && diffType != DifferenceTypeConflict && diffType != DifferenceTypeRemoved {
			diffRec.EntryCtid = &parentEnt.RowCtid
		}
		err = batch.Write(diffRec)
		if err != nil {
			return err
		}
		records++
	}
	if err := parentScanner.Err(); err != nil {
		return err
	}
	return batch.Flush()
}

func lineageCommitIDByBranchID(lineage []lineageCommit, branchID int64) CommitID {
	for _, l := range lineage {
		if l.BranchID == branchID {
			return l.CommitID
		}
	}
	return UncommittedID
}

func evaluateFromParentElementDiffType(childBranchID int64, childLastFromParentCommitID CommitID, childLineage []lineageCommit, parentEnt *DBScannerEntry, matchedChild *DBScannerEntry) DifferenceType {
	// both deleted - none
	if parentEnt.IsDeleted() && (matchedChild == nil || matchedChild.IsDeleted()) {
		return DifferenceTypeNone
	}

	// same entry - none
	if matchedChild != nil && parentEnt.IsDeleted() == matchedChild.IsDeleted() && parentEnt.Checksum == matchedChild.Checksum {
		return DifferenceTypeNone
	}

	// parent not changed - none
	commitIDByChildLineage := lineageCommitIDByBranchID(childLineage, parentEnt.BranchID)
	parentChangedAfterChild := parentEnt.ChangedAfterCommit(commitIDByChildLineage)
	if !parentChangedAfterChild {
		return DifferenceTypeNone
	}

	// child entry is uncommitted or updated after merge from parent - conflict
	if matchedChild != nil && matchedChild.BranchID == childBranchID &&
		(!matchedChild.IsCommitted() || matchedChild.ChangedAfterCommit(childLastFromParentCommitID)) {
		return DifferenceTypeConflict
	}

	// parent deleted - removed
	if parentEnt.IsDeleted() {
		return DifferenceTypeRemoved
	}

	// child delete - add
	if matchedChild == nil || matchedChild.IsDeleted() {
		return DifferenceTypeAdded
	}
	// child exists - change
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

func (c *cataloger) diffFromChild(ctx context.Context, tx db.Tx, childID, parentID int64, limit int, after string) error {
	effectiveCommits, err := c.selectChildEffectiveCommits(tx, childID, parentID)
	if err != nil {
		return err
	}

	diffResultsTableName, err := createDiffResultsTable(ctx, tx)
	if err != nil {
		return err
	}

	scannerOpts := DBScannerOptions{
		After:            after,
		AdditionalFields: []string{DBEntryFieldChecksum},
	}
	childScanner := NewDBBranchScanner(tx, childID, CommittedID, &scannerOpts)
	parentScanner := NewDBLineageScanner(tx, parentID, UncommittedID, &scannerOpts)
	batch := newDiffResultsBatchWriter(tx, diffResultsTableName)
	var parentEnt *DBScannerEntry
	records := 0
	for childScanner.Next() {
		// stop on limit
		if limit > -1 && records >= limit {
			break
		}

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

		diffType := evaluateFromChildElementDiffType(effectiveCommits, parentID, childEnt, parentEnt)
		if diffType == DifferenceTypeNone {
			continue
		}

		err = batch.Write(&diffResultRecord{
			SourceBranch: childID,
			DiffType:     diffType,
			Entry:        childEnt.Entry,
			EntryCtid:    &childEnt.RowCtid,
		})
		if err != nil {
			return err
		}
		records++
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
		return "", fmt.Errorf("diff from child diff result table: %w", err)
	}
	return diffResultsTableName, nil
}

func evaluateFromChildElementDiffType(effectiveCommits *diffEffectiveCommits, parentBranchID int64, childEnt *DBScannerEntry, parentEnt *DBScannerEntry) DifferenceType {
	var matchedParent *DBScannerEntry
	if parentEnt != nil && parentEnt.Path == childEnt.Path {
		matchedParent = parentEnt
	}
	// when the entry was deleted
	if childEnt.IsDeleted() {
		if matchedParent == nil || matchedParent.IsDeleted() {
			return DifferenceTypeNone
		}
		if matchedParent.BranchID == parentBranchID {
			// check if parent did any change to the entity
			if matchedParent.MinCommit > effectiveCommits.ParentEffectiveCommit {
				return DifferenceTypeConflict
			}
			return DifferenceTypeRemoved
		}
		// check if the parent saw this entry at the time
		if matchedParent.MinCommit >= effectiveCommits.ParentEffectiveCommitByBranchID(matchedParent.BranchID) {
			return DifferenceTypeRemoved
		}
		return DifferenceTypeNone
	}
	// when the entry was not deleted
	if matchedParent != nil {
		if matchedParent.IsDeleted() {
			return DifferenceTypeAdded
		}
		if matchedParent.BranchID == parentBranchID {
			// check if parent did any change to the entity
			if matchedParent.MinCommit > effectiveCommits.ParentEffectiveCommit {
				return DifferenceTypeConflict
			}
			return DifferenceTypeChanged
		}
		if matchedParent.MinCommit >= effectiveCommits.ParentEffectiveCommitByBranchID(matchedParent.BranchID) {
			return DifferenceTypeChanged
		}
		return DifferenceTypeAdded
	}

	return DifferenceTypeAdded
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
	effectiveCommitsQuery, args, err := sq.Select(`commit_id AS parent_effective_commit`, `merge_source_commit AS child_effective_commit`).
		From("catalog_commits").
		Where("branch_id = ? AND merge_source_branch = ? AND merge_type = 'from_child'", parentID, childID).
		OrderBy(`commit_id DESC`).
		Limit(1).
		PlaceholderFormat(sq.Dollar).
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

func (c *cataloger) diffNonDirect(_ context.Context, _ db.Tx, leftID, rightID int64, _ int, _ string) error {
	c.log.WithFields(logging.Fields{
		"left_id":  leftID,
		"right_id": rightID,
	}).Debug("Diff not direct - feature not supported")
	return ErrFeatureNotSupported
}
