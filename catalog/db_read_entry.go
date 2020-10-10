package catalog

import (
	"strconv"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

// EntryLineageSelect select path/s from a branch, including lineage
// 1. Union all the branches in the lineage.
// 2. if multiple branches have this path - select the one closest to the requested branch
// 3. filterDeleted param = true will remove results that were deleted
func EntryLineageSelect(tx db.Tx, branchID int64, commitID CommitID, filterDeleted bool, paths []string) (sq.SelectBuilder, error) {
	lineage, err := getLineage(tx, branchID, commitID)
	if err != nil {
		return sq.SelectBuilder{}, err
	}
	unionSelect := EntryBranchSelect(branchID, commitID, paths).Column("? as lineage_order", "0").
		Prefix("(").Suffix(")")
	for i, branch := range lineage {
		unionSelect = unionSelect.SuffixExpr(sq.ConcatExpr(
			" UNION ALL (",
			EntryBranchSelect(branch.BranchID, branch.CommitID, paths).Column("? as lineage_order", strconv.Itoa(i+1)),
			")"))
	}
	distinctSelect := sq.Select("*").
		FromSelect(unionSelect, "c").
		Distinct().Options("ON (path)").
		OrderBy("path", "lineage_order")
	finalSelect := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata", "is_expired").
		FromSelect(distinctSelect, "t")
	if filterDeleted {
		finalSelect = finalSelect.Where("max_commit = ?", MaxCommitID)
	}
	return finalSelect, nil
}

//  EntryBranchSelect select path/s from a single branch.
//  1. Get the requested commit
//  2. If a path have multiple versions in various commits- Select only the relevant one -  with Highest min commit
//	3. if the version was deleted after the requested commit - the row will be set to uncommitted
func EntryBranchSelect(branchID int64, commitID CommitID, paths []string) sq.SelectBuilder {
	rawSelect := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata", "is_expired").
		Distinct().Options("ON (branch_id,path)").
		From("catalog_entries").
		Where("branch_id = ?", branchID).
		OrderBy("branch_id", "path", "min_commit desc")
	l := len(paths)
	// the case of single path is handled differently than multiple pathes because my tests showed there is a significant
	// difference between (about 25%) between "where path = X" and "where path in (X)". even though the optimization is
	// the same. may be a driver issue in the client application
	if l == 1 {
		rawSelect = rawSelect.Where("path = ?", paths[0])
	} else {
		rawSelect = rawSelect.Where(sq.Eq{"path": paths})
	}
	switch commitID {
	case CommittedID:
		rawSelect = rawSelect.Where("min_commit < ?", MaxCommitID).
			Column("max_commit")
	case UncommittedID:
		rawSelect = rawSelect.Column("max_commit")
	default:
		rawSelect = rawSelect.Where("min_commit between 1 and ?", commitID).
			Column("CASE WHEN max_commit >= ? THEN ? ELSE max_commit END AS max_commit", commitID, MaxCommitID)
	}
	return rawSelect
}
