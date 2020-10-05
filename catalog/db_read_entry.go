package catalog

import (
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/treeverse/lakefs/db"
)

type pk struct {
	MinMaxCommit
}

func LineageSelect(branchID int64, paths []string, commitID CommitID, tx db.Tx) sq.SelectBuilder {
	//var topCommitId CommitID
	//if commitID == UncommittedID {
	//	topCommitId = MinCommitUncommittedIndicator
	//} else if commitID == CommittedID {
	//	topCommitId = MinCommitUncommittedIndicator - 1
	//} else {
	//	topCommitId = commitID
	//}
	lineage, err := getLineage(tx, branchID, commitID)
	panicIfErr(err)
	queries := make([]sq.SelectBuilder, len(lineage)+1)
	queries[0] = singleBranchSelect(branchID, paths, commitID).Column("? as lineage_order", 0)
	for i, branch := range lineage {
		queries[i+1] = singleBranchSelect(branch.BranchID, paths, branch.CommitID).Column("? as lineage_order", i+1)
	}
	//from each branch in the lineage,  select the most current entry for the path
	unionSelect := queries[0].Prefix("(").Suffix(")")
	for i := 1; i < len(queries); i++ {
		unionSelect = unionSelect.SuffixExpr(sq.ConcatExpr("\n UNION ALL \n", "(",
			queries[i], ")"))
	}
	s := sq.DebugSqlizer(unionSelect)
	fmt.Print(s)
	// for each path - select the CTID of the entry that is closest to the branch by lineage
	ctidSelect := sq.Select("entry_ctid").
		FromSelect(unionSelect, "c").
		Distinct().Options("ON (path)").
		OrderBy("path", "lineageOrder")
	// select entries matching the ctid list
	entriesSelect := ctidSelect.Prefix("SELECT * FROM catalog_entries WHERE ctid IN \n(").Suffix(")")
	s = sq.DebugSqlizer(entriesSelect)
	fmt.Print(s)
	return entriesSelect
}

func singleBranchSelect(branchID int64, paths []string, commitID CommitID) sq.SelectBuilder {
	rawSelect := sq.Select("branch_id", "path", "ctid as entry_ctid").
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
	if commitID == CommittedID {
		rawSelect = rawSelect.Where("min_commit < ?", MaxCommitID)
	} else if commitID > 0 {
		rawSelect = rawSelect.Where("min_commit between 1 and ?", commitID)
	}
	return rawSelect
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
