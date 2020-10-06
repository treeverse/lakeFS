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

type diffEffectiveCommits struct {
	ParentEffectiveCommit CommitID `db:"parent_effective_commit"` // last commit parent synchronized with child. If non - it is the commit where the child was branched
	ChildEffectiveCommit  CommitID `db:"child_effective_commit"`  // last commit child synchronized to parent. if never - than it is 1 (everything in the child is a change)
}

type contextKey string

func (k contextKey) String() string {
	return string(k)
}

const (
	DiffMaxLimit = 1000

	diffResultsTableNamePrefix = "catalog_diff_results"
	diffResultsInsertBatchSize = 64
	diffReaderBufferSize       = 1024

	contextDiffResultsKey contextKey = "diff_results_key"
)

type diffResultRecord struct {
	SourceBranch int64
	DiffType     DifferenceType
	Path         string
	EntryCtid    string
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
		err = c.doDiff(ctx, tx, leftID, rightID)
		if err != nil {
			return nil, err
		}
		return getDiffDifferences(ctx, tx, limit+1, after)
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, false, err
	}
	differences := res.(Differences)
	hasMore := paginateSlice(&differences, limit)
	return differences, hasMore, nil
}

func (c *cataloger) doDiff(ctx context.Context, tx db.Tx, leftID, rightID int64) error {
	relation, err := getBranchesRelationType(tx, leftID, rightID)
	if err != nil {
		return err
	}
	return c.doDiffByRelation(ctx, tx, relation, leftID, rightID)
}

func (c *cataloger) doDiffByRelation(ctx context.Context, tx db.Tx, relation RelationType, leftID, rightID int64) error {
	switch relation {
	case RelationTypeFromParent:
		return c.diffFromParent(ctx, tx, leftID, rightID)
	case RelationTypeFromChild:
		return c.diffFromChild(ctx, tx, leftID, rightID)
	case RelationTypeNotDirect:
		return c.diffNonDirect(ctx, tx, leftID, rightID)
	default:
		c.log.WithFields(logging.Fields{
			"relation_type": relation,
			"left_id":       leftID,
			"right_id":      rightID,
		}).Debug("Diff by relation - unsupported type")
		return ErrFeatureNotSupported
	}
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

func (c *cataloger) diffFromParent(ctx context.Context, tx db.Tx, parentID, childID int64) error {
	// get the last child commit number of the last parent merge
	// if there is none - then it is  the first merge
	var maxChildMerge CommitID
	childLineage, err := getLineage(tx, childID, UncommittedID)
	if err != nil {
		return fmt.Errorf("child lineage failed: %w", err)
	}
	parentLineage, err := getLineage(tx, parentID, CommittedID)
	if err != nil {
		return fmt.Errorf("parent lineage failed: %w", err)
	}
	maxChildQuery, args, err := sq.Select("MAX(commit_id) as max_child_commit").
		From("catalog_commits").
		Where("branch_id = ? AND merge_type = 'from_parent'", childID).
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return fmt.Errorf("get child last commit sql: %w", err)
	}
	err = tx.Get(&maxChildMerge, maxChildQuery, args...)
	if err != nil {
		return fmt.Errorf("get child last commit failed: %w", err)
	}
	diffResultsTableName, err := diffResultsTableNameFromContext(ctx)
	if err != nil {
		return err
	}
	diffFromParentSQL, args, err := sqDiffFromParentV(parentID, childID, maxChildMerge, parentLineage, childLineage).
		Prefix(`CREATE UNLOGGED TABLE ` + diffResultsTableName + " AS ").
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return fmt.Errorf("diff from parent sql: %w", err)
	}
	if _, err := tx.Exec(diffFromParentSQL, args...); err != nil {
		return fmt.Errorf("select diff from parent: %w", err)
	}
	return nil
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

func (c *cataloger) diffFromChild(ctx context.Context, tx db.Tx, childID, parentID int64) error {
	effectiveCommits, err := c.selectChildEffectiveCommits(childID, parentID, tx)
	if err != nil {
		return err
	}

	// create diff output table
	diffResultsTableName, err := diffResultsTableNameFromContext(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec("CREATE UNLOGGED TABLE " + diffResultsTableName + ` (
		source_branch bigint NOT NULL,
		diff_type integer NOT NULL,
		path character varying COLLATE "C" NOT NULL,
		entry_ctid tid NOT NULL
	)`)
	if err != nil {
		return fmt.Errorf("diff from child diff result table: %w", err)
	}

	childReader := NewDBBranchReader(tx, childID, CommittedID, diffReaderBufferSize, "")
	parentReader := NewDBLineageReader(tx, parentID, UncommittedID, diffReaderBufferSize, "")

	batch := make([]*diffResultRecord, 0, diffResultsInsertBatchSize)
	var parentEnt *DBReaderEntry
	for childReader.Next() {
		childEnt := childReader.Value()
		if !childEnt.ChangedAfterCommit(effectiveCommits.ChildEffectiveCommit) {
			continue
		}

		// get next parent - next parentEnt that path >= child
		for parentEnt == nil || parentEnt.Path < childEnt.Path {
			parentEnt, err = parentReader.Next()
			if err != nil {
				return err
			}
			if parentEnt == nil {
				break
			}
		}

		// diff
		var matchedParent *DBReaderEntry
		if parentEnt != nil && parentEnt.Path == childEnt.Path {
			matchedParent = parentEnt
		}
		diffType := DifferenceTypeAdded
		if childEnt.IsDeleted() {
			switch {
			case matchedParent == nil || matchedParent.IsDeleted():
				diffType = DifferenceTypeNone
			case matchedParent.MinCommit > effectiveCommits.ParentEffectiveCommit:
				diffType = DifferenceTypeConflict
			default:
				diffType = DifferenceTypeRemoved
			}
		} else if matchedParent != nil {
			switch {
			case matchedParent.MinCommit > effectiveCommits.ParentEffectiveCommit:
				diffType = DifferenceTypeConflict
			case matchedParent.IsDeleted():
				diffType = DifferenceTypeAdded
			default:
				diffType = DifferenceTypeChanged
			}
		}

		// skip to next record if no diff was found
		if diffType == DifferenceTypeNone {
			continue
		}
		// batch diff results
		batch = append(batch, &diffResultRecord{
			SourceBranch: childID,
			DiffType:     diffType,
			Path:         childEnt.Path,
			EntryCtid:    childEnt.RowCtid,
		})
		if len(batch) < diffResultsInsertBatchSize {
			continue
		}

		// insert and clear batch
		if err := insertDiffResultsBatch(tx, diffResultsTableName, batch); err != nil {
			return err
		}
		batch = batch[:0]
	}
	if childReader.Err() != nil {
		return childReader.Err()
	}
	if len(batch) > 0 {
		return insertDiffResultsBatch(tx, diffResultsTableName, batch)
	}
	return nil
}

func insertDiffResultsBatch(exerciser sq.Execer, tableName string, batch []*diffResultRecord) error {
	ins := psql.Insert(tableName).Columns("source_branch", "diff_type", "path", "entry_ctid")
	for _, rec := range batch {
		ins = ins.Values(rec.SourceBranch, rec.DiffType, rec.Path, rec.EntryCtid)
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
func (c *cataloger) selectChildEffectiveCommits(childID int64, parentID int64, tx db.Tx) (*diffEffectiveCommits, error) {
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
			Where("branch_id = ? AND merge_source_branch = ?", childID, parentID).
			OrderBy("commit_id").
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

func (c *cataloger) diffNonDirect(_ context.Context, _ db.Tx, leftID, rightID int64) error {
	c.log.WithFields(logging.Fields{
		"left_id":  leftID,
		"right_id": rightID,
	}).Debug("Diff not direct - feature not supported")
	return ErrFeatureNotSupported
}
