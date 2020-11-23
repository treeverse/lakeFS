package mvcc

import (
	"time"

	"github.com/treeverse/lakefs/catalog"
)

type commitLogRaw struct {
	BranchName            string           `db:"branch_name"`
	CommitID              CommitID         `db:"commit_id"`
	PreviousCommitID      CommitID         `db:"previous_commit_id"`
	Committer             string           `db:"committer"`
	Message               string           `db:"message"`
	CreationDate          time.Time        `db:"creation_date"`
	Metadata              catalog.Metadata `db:"metadata"`
	MergeSourceBranchName string           `db:"merge_source_branch_name"`
	MergeSourceCommit     CommitID         `db:"merge_source_commit"`
}

type lineageCommit struct {
	BranchID int64    `db:"branch_id"`
	CommitID CommitID `db:"commit_id"`
}

type entryPathPrefixInfo struct {
	BranchID   int64  `db:"branch_id"`
	PathSuffix string `db:"path_suffix"`
	MinMaxCommit
}

type MinMaxCommit struct {
	MinCommit CommitID `db:"min_commit"`
	MaxCommit CommitID `db:"max_commit"`
}

func (m MinMaxCommit) IsDeleted() bool {
	return m.MaxCommit != MaxCommitID
}
func (m MinMaxCommit) IsTombstone() bool {
	return m.MaxCommit == TombstoneCommitID
}

func (m MinMaxCommit) IsCommitted() bool {
	return m.MinCommit != MaxCommitID
}

func (m MinMaxCommit) ChangedAfterCommit(commitID CommitID) bool {
	// needed for diff, to check if an entry changed after the lineage commit id
	return m.MinCommit > commitID || (m.IsDeleted() && m.MaxCommit >= commitID)
}
