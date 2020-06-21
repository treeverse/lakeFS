package catalog

import (
	"context"
	"errors"
	"fmt"

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
	err := tx.Get(&maxSonMerge, `SELECT COALESCE(MAX(commit_id),0) FROM commits
			WHERE branch_id = $1 AND merge_type = 'from_father'`, rightID)
	if err != nil {
		return nil, err
	}

	const diffSQL = `CREATE TEMP TABLE ` + diffResultsTableName + ` ON COMMIT DROP AS
		SELECT
			CASE
				WHEN DifferenceTypeConflict THEN 3
				WHEN DifferenceTypeRemoved THEN 1
				WHEN DifferenceTypeChanged THEN 2
				ELSE 0
			END AS diff_type,
			path,
			CASE
				WHEN DifferenceTypeChanged AND entry_in_son THEN entry_ctid
				ELSE NULL END AS entry_ctid
	   FROM ( SELECT *
			   FROM ( SELECT f.path,
						f.is_deleted AS DifferenceTypeRemoved,
						s.path IS NOT NULL AS DifferenceTypeChanged,
						s.path IS NOT NULL AND s.source_branch = $2 as entry_in_son,
						COALESCE(s.is_deleted, true) AND f.is_deleted AS both_deleted,
						-- both point to same object, and have the same deletion status
						s.path IS NOT NULL AND f.physical_address = s.physical_address AND f.is_deleted = s.is_deleted AS same_object,
						-- father created or deleted
						f.min_commit > l.effective_commit -- father created after commit
						OR f.max_commit >= l.effective_commit AND f.is_deleted -- father deleted after commit
									AS father_changed,
						-- son created or deleted 
						s.path IS NOT NULL AND s.source_branch = $2 AND
							(NOT s.is_committed -- uncommitted is new
							 OR s.min_commit > $3 -- created after last commit
                             OR (s.max_commit > $3 AND s.is_deleted)) -- deleted after last commit
						  AS DifferenceTypeConflict,
							f.entry_ctid
					   FROM ( SELECT *
							   FROM entries_lineage_committed_v
							  WHERE displayed_branch = $1 AND rank=1) f
						 LEFT JOIN ( SELECT *
							   FROM entries_lineage_full_v 
							  WHERE displayed_branch = $2 and rank=1) s ON f.path = s.path
						 JOIN lineage_v l ON f.source_branch = l.ancestor_branch AND l.branch_id = $2 AND l.active_lineage) t_1
			   WHERE father_changed AND NOT (same_object OR both_deleted) ) t`
	if _, err := tx.Exec(diffSQL, leftID, rightID, maxSonMerge); err != nil {
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

	err := tx.Get(&effectiveCommits, `SELECT commit_id AS father_effective_commit,merge_source_commit AS son_effective_commit 
		FROM commits WHERE branch_id = $1 AND merge_type = 'from_son' ORDER BY commit_id DESC LIMIT 1`, rightID)
	if errors.Is(err, db.ErrNotFound) {
		effectiveCommits.SonEffectiveCommit = 1
		err = tx.Get(&effectiveCommits.FatherEffectiveCommit, `SELECT effective_commit FROM lineage WHERE branch_id = $1 AND ancestor_branch = $2 ORDER BY min_commit DESC LIMIT 1`, leftID, rightID)
	}
	if err != nil {
		return nil, err
	}
	const diffFromSonSQL = `CREATE TEMP TABLE ` + diffResultsTableName + ` ON COMMIT DROP AS
		SELECT CASE
				WHEN DifferenceTypeConflict THEN 3
				WHEN DifferenceTypeRemoved THEN 1
				WHEN DifferenceTypeChanged THEN 2
				ELSE 0
			END AS diff_type,
			path,
			CASE
				WHEN NOT(DifferenceTypeConflict OR DifferenceTypeRemoved) THEN entry_ctid
				ELSE NULL END AS entry_ctid,
			source_branch
	    FROM ( SELECT *
			   FROM ( SELECT s.path,
						s.is_deleted AS DifferenceTypeRemoved,
						f.path IS NOT NULL AS DifferenceTypeChanged,
						  -- can be ignored -father and son entries do not exist
						COALESCE(f.is_deleted, true) AND s.is_deleted AS both_deleted,
						-- can be ignored - both point to same object, and have the same deletion status
						f.path IS NOT NULL AND (f.physical_address = s.physical_address AND f.is_deleted = s.is_deleted) AS same_object,
						-- father either created or deleted after last merge  - conflict
						f.path IS NOT NULL AND ( NOT f.is_committed OR -- uncommitted entries allways new
												(f.source_branch = $1 AND  -- it is the father branch - not from lineage
												( f.min_commit > $3 OR -- created after last merge
												 (f.max_commit >= $3 AND f.is_deleted))) -- deleted after last merge
												OR (f.source_branch != $1 AND  -- an entry from father lineage
							-- negative proof - if the son could see this object - than this is NOT a conflict
							-- done by examining the son lineage against the father object
												 NOT EXISTS ( SELECT * FROM lineage l WHERE
														l.branch_id = $2 AND l.ancestor_branch = f.source_branch AND
													-- prove that ancestor entry  was observable by the son
													  ( $4 BETWEEN l.min_commit AND l.max_commit) AND -- effective lineage on last merge
														(l.effective_commit >= f.min_commit AND
														 (l.effective_commit > f.max_commit OR NOT f.is_deleted))
													   ))) 
														AS DifferenceTypeConflict, -- father changed so it is a conflict
												s.entry_ctid,
							f.source_branch
					   FROM ( SELECT *  -- only entries that were committed after last merge
							   FROM top_committed_entries_v
							  WHERE branch_id = $2 AND (min_commit >= $4 OR max_commit >= $4 and is_deleted)) s
						 LEFT JOIN ( SELECT *
							   FROM entries_lineage_full_v 
							  WHERE displayed_branch = $1 AND rank=1) f
						 ON f.path = s.path
			  ) t WHERE  NOT (same_object OR both_deleted)) t1`
	if _, err := tx.Exec(diffFromSonSQL, rightID, leftID, effectiveCommits.FatherEffectiveCommit, effectiveCommits.SonEffectiveCommit); err != nil {
		return nil, err
	}
	return diffReadDifferences(tx)
}

func (c *cataloger) diffNonDirect(tx db.Tx, leftID, rightID int) (Differences, error) {
	panic("not implemented - Someday is not a day of the week")
}
