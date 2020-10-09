package catalog

import (
	"strconv"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

func LineageSelect(tx db.Tx, branchID int64, commitID CommitID, filterDeleted bool, paths []string) (sq.SelectBuilder, error) {
	lineage, err := getLineage(tx, branchID, commitID)
	if err != nil {
		return sq.Select(), err
	}
	queries := make([]sq.SelectBuilder, len(lineage)+1)
	queries[0] = singleBranchSelect(branchID, paths, commitID).Column("? as lineage_order", "0")
	for i, branch := range lineage {
		queries[i+1] = singleBranchSelect(branch.BranchID, paths, branch.CommitID).Column("? as lineage_order", strconv.Itoa(i+1))
	}
	// from each branch in the lineage,  select the most current entry for the path
	unionSelect := queries[0].Prefix("(").Suffix(")")
	for i := 1; i < len(queries); i++ {
		unionSelect = unionSelect.SuffixExpr(sq.ConcatExpr("\n UNION ALL \n", "(",
			queries[i], ")"))
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

func singleBranchSelect(branchID int64, paths []string, commitID CommitID) sq.SelectBuilder {
	rawSelect := sq.Select("path", "physical_address", "creation_date", "size", "checksum", "metadata", "is_expired").
		Distinct().Options("ON (branch_id,path)").
		From("catalog_entries").
		Where("branch_id = ?", branchID).
		OrderBy("branch_id", "path", "min_commit desc")
	l := len(paths)
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
