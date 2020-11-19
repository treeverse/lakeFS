package mvcc

import (
	"time"

	"github.com/treeverse/lakefs/catalog"
)

type commitLogRaw struct {
	BranchName            string           `db:"branch_name"`
	CommitID              catalog.CommitID `db:"commit_id"`
	PreviousCommitID      catalog.CommitID `db:"previous_commit_id"`
	Committer             string           `db:"committer"`
	Message               string           `db:"message"`
	CreationDate          time.Time        `db:"creation_date"`
	Metadata              catalog.Metadata `db:"metadata"`
	MergeSourceBranchName string           `db:"merge_source_branch_name"`
	MergeSourceCommit     catalog.CommitID `db:"merge_source_commit"`
}

type lineageCommit struct {
	BranchID int64            `db:"branch_id"`
	CommitID catalog.CommitID `db:"commit_id"`
}

type entryPathPrefixInfo struct {
	BranchID   int64  `db:"branch_id"`
	PathSuffix string `db:"path_suffix"`
	catalog.MinMaxCommit
}
