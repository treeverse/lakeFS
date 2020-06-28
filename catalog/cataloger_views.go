package catalog

import (
	"strconv"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

func entriesV(tx db.Tx, requestedCommit CommitID) sq.SelectBuilder {
	entriesQ := sq.Select("*",
		"min_commit > 0 AS is_committed",
		"max_commit = 0 AS is_tombstone",
		"ctid AS entry_ctid\n",
		"max_commit < max_commit_id() AS is_deleted",
		"CASE  WHEN min_commit = 0 THEN max_commit_id() ELSE min_commit END AS commit_weight").
		From("entries")
	switch requestedCommit {
	case UncommittedID: // no further filtering is required
	case CommittedID:
		entriesQ = sq.Select("*").FromSelect(entriesQ, "t2").Where("is_committed")
	default:
		entriesQ = sq.Select("*").FromSelect(entriesQ, "t2").Where("? >=  min_commit and is_committed", requestedCommit)
	}
	return (entriesQ)
}

type lineageCommit struct {
	BranchID int64
	CommitID int64
}

func getLineage(tx db.Tx, branchID int64, effectiveCommit int64) ([]lineageCommit, error) {
	var requestedLineage []lineageCommit
	sql := `select * from (select * from unnest(
				(select lineage from branches where id=$1),
				(select  lineage_commits from commits 
						where branch_id=$1 and merge_type='from_father' and commit_id <= $2
							order by commit_id desc limit 1))) as t(BranchID,CommitID)`
	err := tx.Select(&requestedLineage, sql, branchID, effectiveCommit)
	if err != nil {
		panic(err)
	}
	return requestedLineage, err

}

/*func lineageV(tx db.Tx) sq.SelectBuilder {
	lineageBase := sq.Select().Columns("branch_id",
		"false as main_branch",
		"precedence",
		"ancestor_branch",
		"effective_commit",
		"min_commit",
		"max_commit").
		Column("max_commit = max_commit_id() as active_lineage").
		From("lineage")
	//var baseMaxCommit int

	lineageFromBranch := sq.Select().Columns("id as branch_id",
		"true as main_branch",
		"0 as precedence",
		"id AS ancestor_branch",
		"0 AS min_commit",
		"max_commit_id() as max_commit",
		"true AS active_lineage").
		Column(next_commit - 1 AS effective_commit).
		From("branches")

	sql, args := lineageFromBranch.MustSql()
	return lineageBase.Suffix("UNION ALL "+sql, args...)
}*/

func EntriesLineageFullV(tx db.Tx, branchID int64, requestedCommit CommitID) sq.SelectBuilder {
	effectiveCommit := int64(requestedCommit)
	if requestedCommit <= 0 {
		effectiveCommit = MaxCommitID
	}
	lineage, err := getLineage(tx, branchID, effectiveCommit)
	if err != nil {
		panic(err)
	}
	isDisplayedBranch := "e.branch_id = " + strconv.FormatInt(branchID, 10)
	maxCommitExpr := sq.Case().When(isDisplayedBranch, "e.max_commit\n")
	isDeletedExper := sq.Case().When(isDisplayedBranch, "e.is_deleted\n")
	lineageFilter := "(" + isDisplayedBranch + ")\n"
	for _, lc := range lineage {
		branchCond := "e.branch_id = " + strconv.FormatInt(lc.BranchID, 10)
		commitStr := strconv.FormatInt(lc.CommitID, 10)
		ancestorCond := branchCond + " and e.max_commit <= " + commitStr
		maxCommitExpr = maxCommitExpr.When(ancestorCond, "e.max_commit\n")
		isDeletedExper = isDeletedExper.When(ancestorCond, "e.is_deleted\n")
		lineageFilter += " OR (" + branchCond + " AND e.min_commit <= " + commitStr + " AND e.is_committed) \n"
	}
	maxCommitExpr = maxCommitExpr.Else("max_commit_id()")
	maxCommitAlias := sq.Alias(maxCommitExpr, "max_commit")
	isDeletedExper = isDeletedExper.Else("false")
	isDeletedAlias := sq.Alias(isDeletedExper, "is_deleted")
	baseSelect := sq.Select().Distinct().Options(" ON (e.path) ").
		FromSelect(entriesV(tx, requestedCommit), "e\n").
		Where(lineageFilter).
		OrderBy("e.path", "source_branch desc", "e.commit_weight desc").
		Column("? AS displayed_branch", strconv.FormatInt(branchID, 10)).
		Columns("e.path", "e.branch_id AS source_branch",
			"e.min_commit", "e.physical_address",
			"e.creation_date", "e.size", "e.checksum", "e.metadata",
			"e.is_committed", "e.is_tombstone", "e.entry_ctid").
		Column(maxCommitAlias).Column(isDeletedAlias)

	return baseSelect
}

func diffFromSonV(tx db.Tx, fatherID, sonID, fatherEffectiveCommit, sonEffectiveCommit int64) sq.SelectBuilder {
	fatherSQL, fatherArgs := sq.Select("*").FromSelect(EntriesLineageFullV(tx, int64(fatherID), UncommittedID), "z").
		Where("displayed_branch = ? AND rank=1", fatherID).MustSql()
	fromSonInternalQ := sq.Select("s.path",
		"s.is_deleted AS DifferenceTypeRemoved",
		"f.path IS NOT NULL AS DifferenceTypeChanged",
		"COALESCE(f.is_deleted, true) AND s.is_deleted AS both_deleted",
		"f.path IS NOT NULL AND (f.physical_address = s.physical_address AND f.is_deleted = s.is_deleted) AS same_object",
		"s.entry_ctid",
		"f.source_branch",
	).
		//Conflict detection
		Column(`-- father either created or deleted after last merge  - conflict
			f.path IS NOT NULL AND ( NOT f.is_committed OR -- uncommitted entries allways new
									(f.source_branch = ? AND  -- it is the father branch - not from lineage
									( f.min_commit > ? OR -- created after last merge
									 (f.max_commit >= ? AND f.is_deleted))) -- deleted after last merge
									OR (f.source_branch != ? AND  -- an entry from father lineage
				-- negative proof - if the son could see this object - than this is NOT a conflict
				-- done by examining the son lineage against the father object
									 NOT EXISTS ( SELECT * FROM lineage l WHERE
											l.branch_id = ? AND l.ancestor_branch = f.source_branch AND
										-- prove that ancestor entry  was observable by the son
										  ( ? BETWEEN l.min_commit AND l.max_commit) AND -- effective lineage on last merge
											(l.effective_commit >= f.min_commit AND
											 (l.effective_commit > f.max_commit OR NOT f.is_deleted))
										   ))) 
											AS DifferenceTypeConflict `, fatherID, fatherEffectiveCommit, fatherEffectiveCommit,
			fatherID, sonID, sonEffectiveCommit).
		FromSelect(entriesV(tx, CommittedID).
			Where("branch_id = ? AND (min_commit >= ? OR max_commit >= ? and is_deleted)", sonID, sonEffectiveCommit, sonEffectiveCommit), "s").
		LeftJoin("("+fatherSQL+") AS f ON f.path = s.path", fatherArgs...)
	RemoveNonRelevantQ := sq.Select("*").FromSelect(fromSonInternalQ, "t").Where("NOT (same_object OR both_deleted)")
	return sq.Select().
		Column(sq.Alias(sq.Case().When("DifferenceTypeConflict", "3").
			When("DifferenceTypeRemoved", "1").
			When("DifferenceTypeChanged", "2").
			Else("0"), "diff_type")).
		Column("path").Column(sq.Alias(sq.Case().
		When("NOT(DifferenceTypeConflict OR DifferenceTypeRemoved)", "entry_ctid").
		Else("NULL"),
		"entry_ctid")).
		Column("source_branch").
		FromSelect(RemoveNonRelevantQ, "t1")

}

func diffFromFatherV(tx db.Tx, fatherID, sonID, lastSonCommit int64) sq.SelectBuilder {
	sonSQL, sonArgs := sq.Select("*").FromSelect(EntriesLineageFullV(tx, sonID, UncommittedID), "s").
		Where("displayed_branch = ? and rank=1", sonID).MustSql()
	//lineageSQL, lineageArgs := sq.Select("*").FromSelect(lineageV(tx), "l").
	//	Where("l.branch_id = ? AND l.active_lineage", sonID).MustSql()
	internalV := sq.Select("f.path",
		"f.entry_ctid",
		"f.is_deleted AS DifferenceTypeRemoved",
		"s.path IS NOT NULL AS DifferenceTypeChanged",
		"COALESCE(s.is_deleted, true) AND f.is_deleted AS both_deleted",
		//both point to same object, and have the same deletion status
		"s.path IS NOT NULL AND f.physical_address = s.physical_address AND f.is_deleted = s.is_deleted AS same_object",
		`f.min_commit > l.effective_commit -- father created after commit
			OR f.max_commit >= l.effective_commit AND f.is_deleted -- father deleted after commit
									AS father_changed`).
		Column("s.path IS NOT NULL AND s.source_branch = ? as entry_in_son", sonID).
		Column(`s.path IS NOT NULL AND s.source_branch = ? AND
							(NOT s.is_committed -- uncommitted is new
							 OR s.min_commit > ? -- created after last commit
                           OR (s.max_commit > ? AND s.is_deleted)) -- deleted after last commit
						  AS DifferenceTypeConflict`, sonID, lastSonCommit, lastSonCommit).
		FromSelect(EntriesLineageFullV(tx, fatherID, CommittedID), "f").
		Where("f.displayed_branch = ? AND f.rank=1", fatherID).
		LeftJoin("("+sonSQL+") AS s ON f.path = s.path", sonArgs...)
	//Join("("+lineageSQL+") AS l ON f.source_branch = l.ancestor_branch", lineageArgs...)
	RemoveNonRelevantQ := sq.Select("*").FromSelect(internalV, "t").Where("father_changed AND NOT (same_object OR both_deleted)")

	return sq.Select().
		Column(sq.Alias(sq.Case().When("DifferenceTypeConflict", "3").
			When("DifferenceTypeRemoved", "1").
			When("DifferenceTypeChanged", "2").
			Else("0"), "diff_type")).
		Column("path").
		Column(sq.Alias(sq.Case().
			When("DifferenceTypeChanged AND entry_in_son", "entry_ctid").
			Else("NULL"), "entry_ctid")).
		FromSelect(RemoveNonRelevantQ, "t1")

}
