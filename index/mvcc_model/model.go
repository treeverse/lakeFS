package mvcc_model

import (
	"time"

	"github.com/jackc/pgtype"
)

type JSONMetadata map[string]string

type Repo struct {
	Id               int       `db:"id"`
	name             string    `db:"name"`
	StorageNamespace string    `db:"storage_namespace"`
	CreationDate     time.Time `db:"creation_date"`
	DefaultBranch    int       `db:"default_branch"`
}

type ObjectDedup struct {
	RepositoryId    string `db:"repository_id"`
	DedupId         string `db:"dedup_id"`
	PhysicalAddress string `db:"physical_address"`
}

type Entry struct {
	BranchId        int    `db:"branch_id"`
	Key             string `db:"key"`
	Commits_start   int
	Commits_end     int
	commits         pgtype.Int4range `db:"commits"`
	PhysicalAddress string           `db:"physical_address"`
	CreationDate    time.Time        `db:"creation_date"`
	Size            int64            `db:"size"`
	Checksum        string           `db:"checksum"`
	Metadata        JSONMetadata     `db:"metadata"`
}

type Commit struct {
	BranchId     int          `db:"branch_id"`
	CommitNumber int          `db:"commit_number"`
	Committer    string       `db:"committer"`
	Message      string       `db:"message"`
	CreationDate time.Time    `db:"creation_date"`
	Metadata     JSONMetadata `db:"metadata"`
	//todo: How to treat a merge? is it a different commit type
}

type Branch struct {
	RepositoryId int    `db:"repository_id"`
	Id           int    `db:"id"`
	Name         string `db:"name"`
	NextCommit   int    `db:"next_commit"`
}

type MultipartUpload struct {
	BranchId     int       `db:"branch_id"`
	UploadId     string    `db:"upload_id"`
	Path         string    `db:"path"`
	CreationDate time.Time `db:"creation_date"`
}

type Lineage struct {
	BranchId        int `db:"branch_id"`
	Precedence      int `db:"precedence"`
	AncestorBranch  int `db:"ancestor_branch"`
	EffectiveCommit int `db:"effective_commit"`
	Commits_start   int
	Commits_end     int
	branchCommits   pgtype.Int4range `db:"branch_commits"`
}
