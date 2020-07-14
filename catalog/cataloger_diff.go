package catalog

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

const diffResultsTableName = "diff_results"

func (c *cataloger) Diff(ctx context.Context, repository string, leftBranch string, rightBranch string) (Differences, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "leftBranch", IsValid: ValidateBranchName(leftBranch)},
		{Name: "rightBranch", IsValid: ValidateBranchName(rightBranch)},
	}); err != nil {
		return nil, err
	}
	differences, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		leftID, err := getBranchID(tx, repository, leftBranch, LockTypeNone)
		if err != nil {
			return nil, fmt.Errorf("left branch: %w", err)
		}
		rightID, err := getBranchID(tx, repository, rightBranch, LockTypeNone)
		if err != nil {
			return nil, fmt.Errorf("right branch: %w", err)
		}
		return c.doDiff(tx, leftID, rightID)
	}, c.txOpts(ctx)...)
	if err != nil {
		return nil, err
	}
	return differences.(Differences), nil
}

func (c *cataloger) doDiff(tx db.Tx, leftID, rightID int64) (Differences, error) {
	relation, err := getBranchesRelationType(tx, leftID, rightID)
	if err != nil {
		return nil, err
	}
	return c.doDiffByRelation(tx, relation, leftID, rightID)
}

func (c *cataloger) doDiffByRelation(tx db.Tx, relation RelationType, leftID, rightID int64) (Differences, error) {
	switch relation {
	case RelationTypeFromFather:
		return c.diffFromFather(tx, leftID, rightID)
	case RelationTypeFromSon:
		return c.diffFromSon(tx, leftID, rightID)
	case RelationTypeNotDirect:
		return c.diffNonDirect(tx, leftID, rightID)
	default:
		return nil, nil
	}
}

func (c *cataloger) diffFromFather(tx db.Tx, fatherID, sonID int64) (Differences, error) {
	// get the last son commit number of the last father merge
	// if there is none - then it is  the first merge
	var maxSonMerge int64
	sonLineage, err := getLineage(tx, sonID, UncommittedID)
	if err != nil {
		return nil, fmt.Errorf("son lineage failed: %w", err)
	}
	fatherLineage, err := getLineage(tx, fatherID, CommittedID)
	if err != nil {
		return nil, fmt.Errorf("father lineage failed: %w", err)
	}
	maxSonQuery, args := sq.Select("COALESCE(MAX(commit_id),0) as max_on_commit"). //TODO:99i-0
											From("commits").
											Where("branch_id = ? AND merge_type = 'from_father'", sonID).
											PlaceholderFormat(sq.Dollar).MustSql()
	err = tx.Get(&maxSonMerge, maxSonQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("get son last commit failed: %w", err)
	}

	s, args, err := sqDiffFromFatherV(fatherID, sonID, maxSonMerge, fatherLineage, sonLineage).
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return nil, err
	}

	diffFromFatherSQL := `CREATE TEMP TABLE ` + diffResultsTableName + " ON COMMIT DROP AS " + s

	if _, err := tx.Exec(diffFromFatherSQL, args...); err != nil {
		return nil, err
	}
	return diffReadDifferences(tx)
}

func diffReadDifferences(tx db.Tx) (Differences, error) {
	var result Differences
	if err := tx.Select(&result, "SELECT diff_type, path FROM "+diffResultsTableName); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *cataloger) diffFromSon(tx db.Tx, sonID, fatherID int64) (Differences, error) {
	// read last merge commit numbers from commit table
	// if it is the first son-to-father commit, than those commit numbers are calculated as follows:
	// the son is 0, as any change in the some was never merged to the father.
	// the father is lhe effective commit number of the first lineage record of the son that points to the father
	// it is possible that the son the have already done from_father merge. so we have to take the minimal effective commit
	effectiveCommits := struct {
		FatherEffectiveCommit CommitID `db:"father_effective_commit"` // last commit father synchronized with son. If non - it is the commit where the son was branched
		SonEffectiveCommit    CommitID `db:"son_effective_commit"`    // last commit son synchronized to father. if never - than it is 1 (everything in the son is a change)
	}{}

	effectiveCommitsQuery, args, err := sq.Select(` commit_id AS father_effective_commit`, `merge_source_commit AS son_effective_commit`).
		From("commits").
		Where("branch_id = ? AND merge_source_branch = ? AND merge_type = 'from_son'", sonID, fatherID).
		OrderBy(`commit_id DESC`).
		Limit(1).PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return nil, err
	}
	err = tx.Get(&effectiveCommits, effectiveCommitsQuery, args...)
	if errors.Is(err, db.ErrNotFound) {
		effectiveCommits.SonEffectiveCommit = 1 // we need all commits from the son. so any small number will do
		query := sq.Select("commit_id as father_effective_commit").From("commits").
			Where("branch_id = ? AND merge_source_branch = ?", sonID, fatherID).
			OrderBy("commit_id").Limit(1)
		FatherEffectiveQuery, args := query.PlaceholderFormat(sq.Dollar).MustSql()
		err = tx.Get(&effectiveCommits.FatherEffectiveCommit, FatherEffectiveQuery, args...)
	}
	if err != nil {
		return nil, err
	}

	fatherLineage, err := getLineage(tx, fatherID, UncommittedID)
	if err != nil {
		return nil, fmt.Errorf("father lineage failed: %w", err)
	}
	sonLineage, err := getLineage(tx, sonID, CommittedID)
	if err != nil {
		return nil, fmt.Errorf("son lineage failed: %w", err)
	}
	sonLineageValues := getLineageAsValues(sonLineage, sonID)

	diffExpr := sqDiffFromSonV(fatherID, sonID, effectiveCommits.FatherEffectiveCommit, effectiveCommits.SonEffectiveCommit, fatherLineage, sonLineageValues)

	s, args, err := diffExpr.PlaceholderFormat(sq.Dollar).ToSql()
	if err != nil {
		return nil, err
	}

	diffFromSonSQL := `CREATE TEMP TABLE ` + diffResultsTableName + " ON COMMIT DROP AS " + s

	if _, err := tx.Exec(diffFromSonSQL, args...); err != nil {
		return nil, err
	}
	return diffReadDifferences(tx)
}

func (c *cataloger) diffNonDirect(tx db.Tx, leftID, rightID int64) (Differences, error) {
	panic("not implemented - Someday is not a day of the week")
}
