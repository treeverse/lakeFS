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
	}, c.txOpts(ctx, db.ReadOnly())...)
	if err != nil {
		return nil, err
	}
	return differences.(Differences), nil
}

func (c *cataloger) doDiff(tx db.Tx, leftID, rightID int) (Differences, error) {
	relation, err := getBranchesRelationType(tx, leftID, rightID)
	if err != nil {
		return nil, err
	}
	return c.doDiffByRelation(tx, relation, leftID, rightID)
}

func (c *cataloger) doDiffByRelation(tx db.Tx, relation RelationType, leftID, rightID int) (Differences, error) {
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

func (c *cataloger) diffFromFather(tx db.Tx, leftID, rightID int) (Differences, error) {
	// get the last son commit number of the last father merge
	// if there is none - then it is  the first merge
	var maxSonMerge int
	maxSonQuery, args := sq.Select("COALESCE(MAX(commit_id),0) as max_on_commit"). //TODO:99i-0
											From("commits").
											Where("branch_id = ? AND merge_type = 'from_father'", rightID).
											PlaceholderFormat(sq.Dollar).MustSql()
	err := tx.Get(&maxSonMerge, maxSonQuery, args...)
	//err := tx.Get(&maxSonMerge, `SELECT COALESCE(MAX(commit_id),0) FROM commits
	//		WHERE branch_id = $1 AND merge_type = 'from_father'`, rightID)
	if err != nil {
		return nil, err
	}

	s, args := diffFromFatherV(tx, leftID, rightID, maxSonMerge).PlaceholderFormat(sq.Dollar).MustSql()

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

func (c *cataloger) diffFromSon(tx db.Tx, leftID, rightID int) (Differences, error) {
	// read last merge commit numbers from commit table
	// if it is the first son-to-father commit, than those commit numbers are calculated as follows:
	// the son is 0, as any change in the some was never merged to the father.
	// the father is lhe effective commit number of the first lineage record of the son that points to the father
	// it is possible that the son the have already done from_father merge. so we have to take the minimal effective commit
	effectiveCommits := struct {
		FatherEffectiveCommit int `db:"father_effective_commit"`
		SonEffectiveCommit    int `db:"son_effective_commit"`
	}{}

	effectiveCommitsQuery, args := sq.Select(` commit_id AS father_effective_commit`, `merge_source_commit AS son_effective_commit`).
		From("commits").
		Where("branch_id = ? AND merge_type = 'from_son'", rightID).
		OrderBy(`commit_id DESC`).
		Limit(1).PlaceholderFormat(sq.Dollar).
		MustSql()

	err := tx.Get(&effectiveCommits, effectiveCommitsQuery, args...)

	if errors.Is(err, db.ErrNotFound) {
		effectiveCommits.SonEffectiveCommit = 1
		FatherEffectiveQuery, args := sq.Select("effective_commit").From("lineage").
			Where("branch_id = ? AND ancestor_branch = ?", leftID, rightID).
			OrderBy("min_commit DESC").Limit(1).PlaceholderFormat(sq.Dollar).MustSql()
		//err = tx.Get(&effectiveCommits.FatherEffectiveCommit, `SELECT effective_commit FROM lineage WHERE branch_id = $1 AND ancestor_branch = $2 ORDER BY min_commit DESC LIMIT 1`, leftID, rightID)
		err = tx.Get(&effectiveCommits.FatherEffectiveCommit, FatherEffectiveQuery, args...)
	}
	if err != nil {
		return nil, err
	}

	s, args := diffFromSonV(tx, rightID, leftID, effectiveCommits.FatherEffectiveCommit, effectiveCommits.SonEffectiveCommit).PlaceholderFormat(sq.Dollar).MustSql()

	diffFromSonSQL := `CREATE TEMP TABLE ` + diffResultsTableName + " ON COMMIT DROP AS " + s

	if _, err := tx.Exec(diffFromSonSQL, args...); err != nil {
		return nil, err
	}
	return diffReadDifferences(tx)
}

func (c *cataloger) diffNonDirect(tx db.Tx, leftID, rightID int) (Differences, error) {
	panic("not implemented - Someday is not a day of the week")
}
