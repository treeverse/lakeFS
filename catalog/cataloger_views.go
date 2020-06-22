package catalog

import (
	sq "github.com/Masterminds/squirrel"
	//sq "github.com/Masterminds/squirrel"
	//_ "github.com/jackc/pgx/stdlib"
	//"github.com/treeverse/lakefs/db"
	//"github.com/jmoiron/sqlx"
)

const MaxCommitIDs = "x'7fffffff'::integer"

func entriesV(committedOnly bool) sq.SelectBuilder {
	entriesQ := sq.Select("*",
		"min_commit <> 0 AS is_committed",
		"max_commit < min_commit OR max_commit = 0 AS is_tombstone",
		"ctid AS entry_ctid").
		Column("max_commit <> " + MaxCommitIDs + " AS is_deleted").
		From("entries")
	if committedOnly {
		entriesQ = sq.Select("*").FromSelect(entriesQ, "t2").Where("is_committed")
	}
	return (entriesQ)
}

func lineageV() sq.SelectBuilder {
	lineageBase := sq.Select().Columns("branch_id",
		"false as main_branch",
		"precedence",
		"ancestor_branch",
		"effective_commit",
		"min_commit",
		"max_commit").
		Column("max_commit = " + MaxCommitIDs + " as active_lineage").
		From("lineage")

	lineageFromBranch := sq.Select().Columns("id as branch_id",
		"true as main_branch",
		"0 as precedence",
		"id AS ancestor_branch",
		"next_commit - 1 AS effective_commit",
		"0 AS min_commit").
		Column(MaxCommitIDs + " as max_commit").
		Column("true AS active_lineage").
		From("branches")

	sql, args := lineageFromBranch.MustSql()
	return lineageBase.Suffix("UNION ALL "+sql, args...)
}

func entriesLineageFullV(committedOnly bool) sq.SelectBuilder {
	lineageSQL, _ := lineageV().MustSql()
	return sq.Select("l.branch_id AS displayed_branch",
		"e.branch_id AS source_branch",
		"e.path", "e.min_commit", "e.physical_address",
		"e.creation_date", "e.size", "e.checksum", "e.metadata", "l.precedence",
		"l.min_commit AS branch_min_commit",
		"l.max_commit AS branch_max_commit",
		"e.is_committed", "l.active_lineage", "l.effective_commit",
		"e.is_tombstone", "e.entry_ctid").
		Column(sq.Alias(sq.Case().
			When("l.main_branch", "e.max_commit").
			When("e.max_commit <= l.effective_commit", "e.max_commit").
			Else(MaxCommitIDs), "max_commit")).
		Column(`row_number() OVER (PARTITION BY l.branch_id, e.path 
							ORDER BY l.precedence, 
							(CASE
						WHEN l.main_branch AND e.min_commit = 0 THEN '01111111111111111111111111111111'::"bit"::integer
						ELSE e.min_commit
						END) DESC) AS rank`).
		Column("e.max_commit <= l.effective_commit AS is_deleted").
		FromSelect(entriesV(committedOnly), "e").
		Join("(" + lineageSQL + ") AS l ON l.ancestor_branch = e.branch_id").
		Where("(l.main_branch OR e.min_commit <= l.effective_commit AND e.is_committed)").
		Where("l.active_lineage")
}

func diffFromSonV(fatherID, sonID, fatherEffectiveCommit, sonEffectiveCommit int) sq.SelectBuilder {
	fatherSQL, fatherArgs := sq.Select("*").FromSelect(entriesLineageFullV(false), "z").
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
		FromSelect(entriesLineageFullV(true).
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

//func diffFromFatherV(fatherID, sonID, lastSonCommit int) sq.SelectBuilder {
//	sonSQL, sonArgs := sq.Select("*").FromSelect(entriesLineageFullV(false), "s").
//		Where("displayed_branch = $ and rank=1", sonID).MustSql()
//	lineageSQL,lineageArgs := sq.Select("*").FromSelect(lineageV(),"l").
//		Where("l.branch_id = $ AND l.active_lineage",sonID).MustSql()
//	internalV := sq.Select("f.path", "f.entry_ctid",
//		"s.path IS NOT NULL AS DifferenceTypeChanged",
//		"COALESCE(s.is_deleted, true) AND f.is_deleted AS both_deleted",
//		//both point to same object, and have the same deletion status
//		"s.path IS NOT NULL AND f.physical_address = s.physical_address AND f.is_deleted = s.is_deleted AS same_object",
//		`f.min_commit > l.effective_commit -- father created after commit
//			OR f.max_commit >= l.effective_commit AND f.is_deleted -- father deleted after commit
//									AS father_changed`).
//		Column("s.path IS NOT NULL AND s.source_branch = ? as entry_in_son", sonID).
//		Column(`s.path IS NOT NULL AND s.source_branch = ? AND
//							(NOT s.is_committed -- uncommitted is new
//							 OR s.min_commit > ? -- created after last commit
//                             OR (s.max_commit > ? AND s.is_deleted)) -- deleted after last commit
//						  AS DifferenceTypeConflict`, sonID, lastSonCommit, lastSonCommit).
//		FromSelect(entriesLineageFullV(true).
//			Where("displayed_branch = $ AND rank=1", fatherID), "f").
//		LeftJoin("("+sonSQL+") AS s ON f.path = s.path", sonArgs...).
//		Join("("+lineageSQL+") AS l ON f.source_branch = l.ancestor_branch")
//	RemoveNonRelevantQ := sq.Select("*").FromSelect(internalV,"t").Where(("father_changed AND NOT (same_object OR both_deleted)"))
//
//}
