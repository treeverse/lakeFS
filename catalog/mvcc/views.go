package mvcc

import (
	"fmt"
	"strconv"
	"unicode/utf8"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/catalog"
)

const (
	DirectoryTerminationValue = utf8.MaxRune
	DirectoryTermination      = string(rune(DirectoryTerminationValue))
)

func sqEntriesV(requestedCommit catalog.CommitID) sq.SelectBuilder {
	entriesQ := sq.Select("*",
		fmt.Sprintf("min_commit != %d AS is_committed", catalog.MinCommitUncommittedIndicator),
		"max_commit = 0 AS is_tombstone",
		"ctid AS entry_ctid\n",
		fmt.Sprintf("max_commit < %d AS is_deleted", catalog.MaxCommitID)).
		From("catalog_entries")
	switch requestedCommit {
	case catalog.UncommittedID: // no further filtering is required
	case catalog.CommittedID:
		entriesQ = sq.Select("*").FromSelect(entriesQ, "t2").Where("is_committed")
	default:
		entriesQ = sq.Select("*").FromSelect(entriesQ, "t2").Where("? >=  min_commit and is_committed", requestedCommit)
	}
	return entriesQ
}

func sqEntriesLineage(branchID int64, requestedCommit catalog.CommitID, lineage []lineageCommit) sq.SelectBuilder {
	isDisplayedBranch := "e.branch_id = " + strconv.FormatInt(branchID, 10)
	maxCommitExpr := sq.Case().When(isDisplayedBranch, "e.max_commit\n")
	isDeletedExpr := sq.Case().When(isDisplayedBranch, "e.is_deleted\n")
	lineageFilter := "(" + isDisplayedBranch + ")\n"
	for _, lc := range lineage {
		branchCond := "e.branch_id = " + strconv.FormatInt(lc.BranchID, 10)
		commitStr := strconv.FormatInt(int64(lc.CommitID), 10)
		ancestorCond := branchCond + " and e.max_commit < " + commitStr
		maxCommitExpr = maxCommitExpr.When(ancestorCond, "e.max_commit\n")
		isDeletedExpr = isDeletedExpr.When(ancestorCond, "e.is_deleted\n")
		lineageFilter += " OR (" + branchCond + " AND e.min_commit <= " + commitStr + " AND e.is_committed) \n"
	}
	maxCommitExpr = maxCommitExpr.Else(sq.Expr("?", catalog.MaxCommitID))
	maxCommitAlias := sq.Alias(maxCommitExpr, "max_commit")
	isDeletedExpr = isDeletedExpr.Else("false")
	isDeletedAlias := sq.Alias(isDeletedExpr, "is_deleted")
	baseSelect := sq.Select().Distinct().Options(" ON (e.path) ").
		FromSelect(sqEntriesV(requestedCommit), "e\n").
		Where(lineageFilter).
		OrderBy("e.path", "source_branch desc", "e.min_commit desc").
		Columns(strconv.FormatInt(branchID, 10)+" AS displayed_branch",
			"e.path", "e.branch_id AS source_branch",
			"e.min_commit", "e.physical_address",
			"e.creation_date", "e.size", "e.checksum", "e.metadata",
			"e.is_committed", "e.is_tombstone", "e.entry_ctid", "e.is_expired").
		Column(maxCommitAlias).Column(isDeletedAlias)
	return baseSelect
}

func sqLineageConditions(branchID int64, lineage []lineageCommit) (string, sq.Sqlizer, sq.Sqlizer) {
	isDisplayedBranch := "e.branch_id = " + strconv.FormatInt(branchID, 10)
	maxCommitExpr := sq.Case().When(isDisplayedBranch, "e.max_commit\n")
	isDeletedExpr := sq.Case().When(isDisplayedBranch, "e.is_deleted\n")
	lineageFilter := "(" + isDisplayedBranch + ")\n"
	for _, lc := range lineage {
		branchCond := "e.branch_id = " + strconv.FormatInt(lc.BranchID, 10)
		commitStr := strconv.FormatInt(int64(lc.CommitID), 10)
		ancestorCond := branchCond + " and e.max_commit < " + commitStr
		maxCommitExpr = maxCommitExpr.When(ancestorCond, "e.max_commit\n")
		isDeletedExpr = isDeletedExpr.When(ancestorCond, "e.is_deleted\n")
		lineageFilter += " OR (" + branchCond + " AND e.min_commit <= " + commitStr + " AND e.is_committed) \n"
	}
	maxCommitExpr = maxCommitExpr.Else(sq.Expr("?", catalog.MaxCommitID))
	maxCommitAlias := sq.Alias(maxCommitExpr, "max_commit")
	isDeletedExpr = isDeletedExpr.Else("false")
	isDeletedAlias := sq.Alias(isDeletedExpr, "is_deleted")
	return lineageFilter, maxCommitAlias, isDeletedAlias
}

func sqEntriesLineageV(branchID int64, requestedCommit catalog.CommitID, lineage []lineageCommit) sq.SelectBuilder {
	lineageFilter,
		maxCommitAlias,
		isDeletedAlias := sqLineageConditions(branchID, lineage)
	baseSelect := sq.Select().Distinct().Options(" ON (e.path) ").
		FromSelect(sqEntriesV(requestedCommit), "e\n").
		Where(lineageFilter).
		OrderBy("e.path", "source_branch desc", "e.min_commit desc").
		Column("? AS displayed_branch", strconv.FormatInt(branchID, 10)).
		Columns("e.path", "e.branch_id AS source_branch",
			"e.min_commit", "e.physical_address",
			"e.creation_date", "e.size", "e.checksum", "e.metadata",
			"e.is_committed", "e.is_tombstone", "e.entry_ctid", "e.is_expired").
		Column(maxCommitAlias).Column(isDeletedAlias)
	return baseSelect
}
