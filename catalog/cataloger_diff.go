package catalog

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
)

const diffResultsTableName = "catalog_diff_results"

func (c *cataloger) Diff(ctx context.Context, repository string, leftBranch string, rightBranch string) (Differences, error) {
	if err := Validate(ValidateFields{
		{Name: "repository", IsValid: ValidateRepositoryName(repository)},
		{Name: "leftBranch", IsValid: ValidateBranchName(leftBranch)},
		{Name: "rightBranch", IsValid: ValidateBranchName(rightBranch)},
	}); err != nil {
		return nil, err
	}
	differences, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		leftID, err := c.getBranchIDCache(tx, repository, leftBranch)
		if err != nil {
			return nil, fmt.Errorf("left branch: %w", err)
		}
		rightID, err := c.getBranchIDCache(tx, repository, rightBranch)
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
		c.log.WithFields(logging.Fields{
			"relation_type": relation,
			"left_id":       leftID,
			"right_id":      rightID,
		}).Debug("Diff by relation - unsupported type")
		return nil, ErrFeatureNotSupported
	}
}

func (c *cataloger) diffFromFather(tx db.Tx, fatherID, sonID int64) (Differences, error) {
	// get the last son commit number of the last father merge
	// if there is none - then it is  the first merge
	var maxSonMerge CommitID
	sonLineage, err := getLineage(tx, sonID, UncommittedID)
	if err != nil {
		return nil, fmt.Errorf("son lineage failed: %w", err)
	}
	fatherLineage, err := getLineage(tx, fatherID, CommittedID)
	if err != nil {
		return nil, fmt.Errorf("father lineage failed: %w", err)
	}
	maxSonQuery, args, err := sq.Select("MAX(commit_id) as max_son_commit").
		From("catalog_commits").
		Where("branch_id = ? AND merge_type = 'from_father'", sonID).
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("get son last commit sql: %w", err)
	}
	err = tx.Get(&maxSonMerge, maxSonQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("get son last commit failed: %w", err)
	}
	diffFromFatherSQL, args, err := sqDiffFromFatherV(fatherID, sonID, maxSonMerge, fatherLineage, sonLineage).
		Prefix(`CREATE TEMP TABLE ` + diffResultsTableName + " ON COMMIT DROP AS ").
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("diff from father sql: %w", err)
	}
	if _, err := tx.Exec(diffFromFatherSQL, args...); err != nil {
		return nil, fmt.Errorf("select diff from father: %w", err)
	}
	return diffReadDifferences(tx)
}

func diffReadDifferences(tx db.Tx) (Differences, error) {
	var result Differences
	if err := tx.Select(&result, "SELECT diff_type, path FROM "+diffResultsTableName); err != nil {
		return nil, fmt.Errorf("select diff results: %w", err)
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

	effectiveCommitsQuery, args, err := sq.Select(`commit_id AS father_effective_commit`, `merge_source_commit AS son_effective_commit`).
		From("catalog_commits").
		Where("branch_id = ? AND merge_source_branch = ? AND merge_type = 'from_son'", fatherID, sonID).
		OrderBy(`commit_id DESC`).
		Limit(1).
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("effective commits sql: %w", err)
	}
	err = tx.Get(&effectiveCommits, effectiveCommitsQuery, args...)
	effectiveCommitsNotFound := errors.Is(err, db.ErrNotFound)
	if err != nil && !effectiveCommitsNotFound {
		return nil, fmt.Errorf("select effective commit: %w", err)
	}
	if effectiveCommitsNotFound {
		effectiveCommits.SonEffectiveCommit = 1 // we need all commits from the son. so any small number will do
		fatherEffectiveQuery, args, err := psql.Select("commit_id as father_effective_commit").
			From("catalog_commits").
			Where("branch_id = ? AND merge_source_branch = ?", sonID, fatherID).
			OrderBy("commit_id").
			Limit(1).
			ToSql()
		if err != nil {
			return nil, fmt.Errorf("father effective commit sql: %w", err)
		}
		err = tx.Get(&effectiveCommits.FatherEffectiveCommit, fatherEffectiveQuery, args...)
		if err != nil {
			return nil, fmt.Errorf("select father effective commit: %w", err)
		}
	}

	fatherLineage, err := getLineage(tx, fatherID, UncommittedID)
	if err != nil {
		return nil, fmt.Errorf("father lineage failed: %w", err)
	}
	sonLineage, err := getLineage(tx, sonID, CommittedID)
	if err != nil {
		return nil, fmt.Errorf("son lineage failed: %w", err)
	}

	sonLineageValues := getLineageAsValues(sonLineage, sonID, MaxCommitID)
	mainDiffFromSon := sqDiffFromSonV(fatherID, sonID, effectiveCommits.FatherEffectiveCommit, effectiveCommits.SonEffectiveCommit, fatherLineage, sonLineageValues)
	diffFromSonSQL, args, err := mainDiffFromSon.
		Prefix("CREATE TEMP TABLE " + diffResultsTableName + " ON COMMIT DROP AS").
		PlaceholderFormat(sq.Dollar).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("diff from son sql: %w", err)
	}
	if _, err := tx.Exec(diffFromSonSQL, args...); err != nil {
		return nil, fmt.Errorf("exec diff from son: %w", err)
	}
	return diffReadDifferences(tx)
}

func (c *cataloger) diffNonDirect(_ db.Tx, leftID, rightID int64) (Differences, error) {
	c.log.WithFields(logging.Fields{
		"left_id":  leftID,
		"right_id": rightID,
	}).Debug("Diff not direct - feature not supported")
	return nil, ErrFeatureNotSupported
}
