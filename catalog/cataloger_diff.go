package catalog

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/rs/xid"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

type contextKey string

func (k contextKey) String() string {
	return string(k)
}

const (
	DiffMaxLimit = 1000

	diffResultsTableNamePrefix            = "catalog_diff_results"
	contextDiffResultsKey      contextKey = "diff_results_key"
)

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

	ctx, cancel := contextWithDiffResultsDispose(ctx, c.db)
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
	// read last merge commit numbers from commit table
	// if it is the first child-to-parent commit, than those commit numbers are calculated as follows:
	// the child is 0, as any change in the child was never merged to the parent.
	// the parent is the effective commit number of the first lineage record of the child that points to the parent
	// it is possible that the child the have already done from_parent merge. so we have to take the minimal effective commit
	effectiveCommits := struct {
		ParentEffectiveCommit CommitID `db:"parent_effective_commit"` // last commit parent synchronized with child. If non - it is the commit where the child was branched
		ChildEffectiveCommit  CommitID `db:"child_effective_commit"`  // last commit child synchronized to parent. if never - than it is 1 (everything in the child is a change)
	}{}

	effectiveCommitsQuery, args, err := sq.Select(`commit_id AS parent_effective_commit`, `merge_source_commit AS child_effective_commit`).
		From("catalog_commits").
		Where("branch_id = ? AND merge_source_branch = ? AND merge_type = 'from_child'", parentID, childID).
		OrderBy(`commit_id DESC`).
		Limit(1).
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return fmt.Errorf("effective commits sql: %w", err)
	}
	err = tx.Get(&effectiveCommits, effectiveCommitsQuery, args...)
	effectiveCommitsNotFound := errors.Is(err, db.ErrNotFound)
	if err != nil && !effectiveCommitsNotFound {
		return fmt.Errorf("select effective commit: %w", err)
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
			return fmt.Errorf("parent effective commit sql: %w", err)
		}
		err = tx.Get(&effectiveCommits.ParentEffectiveCommit, parentEffectiveQuery, args...)
		if err != nil {
			return fmt.Errorf("select parent effective commit: %w", err)
		}
	}

	parentLineage, err := getLineage(tx, parentID, UncommittedID)
	if err != nil {
		return fmt.Errorf("parent lineage failed: %w", err)
	}
	childLineage, err := getLineage(tx, childID, CommittedID)
	if err != nil {
		return fmt.Errorf("child lineage failed: %w", err)
	}

	childLineageValues := getLineageAsValues(childLineage, childID, MaxCommitID)
	mainDiffFromChild := sqDiffFromChildV(parentID, childID, effectiveCommits.ParentEffectiveCommit, effectiveCommits.ChildEffectiveCommit, parentLineage, childLineageValues)
	diffResultsTableName, err := diffResultsTableNameFromContext(ctx)
	if err != nil {
		return err
	}
	diffFromChildSQL, args, err := mainDiffFromChild.
		Prefix("CREATE UNLOGGED TABLE " + diffResultsTableName + " AS ").
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return fmt.Errorf("diff from child sql: %w", err)
	}
	if _, err := tx.Exec(diffFromChildSQL, args...); err != nil {
		return fmt.Errorf("exec diff from child: %w", err)
	}
	return nil
}

// contextWithDiffResultsDispose generate diff results id used for temporary table name
func contextWithDiffResultsDispose(ctx context.Context, tx db.Tx) (context.Context, context.CancelFunc) {
	id := xid.New().String()
	return context.WithValue(ctx, contextDiffResultsKey, id), func() {
		tableName := diffResultsTableNameFormat(id)
		_, _ = tx.Exec("DROP TABLE IF EXISTS " + tableName)
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
