package catalog

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

type Metadata map[string]string

type Repo struct {
	Name             string    `db:"name"`
	StorageNamespace string    `db:"storage_namespace"`
	DefaultBranch    string    `db:"default_branch"`
	CreationDate     time.Time `db:"creation_date"`
}

type Entry struct {
	BranchID        int       `db:"branch_id"`
	Path            string    `db:"path"`
	MinCommit       int       `db:"min_commit"`
	MaxCommit       int       `db:"max_commit"`
	PhysicalAddress string    `db:"physical_address"`
	CreationDate    time.Time `db:"creation_date"`
	Size            int64     `db:"size"`
	Checksum        string    `db:"checksum"`
	Metadata        Metadata  `db:"metadata"`
	IsTombstone     bool      `db:"is_tombstone"`
}

type CommitLog struct {
	Branch       string    `db:"branch"`
	CommitID     int       `db:"commit_id"`
	Committer    string    `db:"committer"`
	Message      string    `db:"message"`
	CreationDate time.Time `db:"creation_date"`
	Metadata     Metadata  `db:"metadata"`
}

type Branch struct {
	Repository string `db:"repository"`
	Name       string `db:"name"`
}

type MultipartUpload struct {
	Repository      string    `db:"repository"`
	UploadID        string    `db:"upload_id"`
	Path            string    `db:"path"`
	CreationDate    time.Time `db:"creation_date"`
	PhysicalAddress string    `db:"physical_address"`
}

type Lineage struct {
	BranchID        int `db:"branch_id"`
	Precedence      int `db:"precedence"`
	AncestorBranch  int `db:"ancestor_branch"`
	EffectiveCommit int `db:"effective_commit"`
	MinCommit       int `db:"min_commit"`
	MaxCommit       int `db:"max_commit"`
}

func (j Metadata) Value() (driver.Value, error) {
	if j == nil {
		return json.Marshal(struct{}{})
	}
	return json.Marshal(j)
}

func (j *Metadata) Scan(src interface{}) error {
	if src == nil {
		return nil
	}
	data, ok := src.([]byte)
	if !ok {
		return errors.New("invalid metadata src format")
	}
	return json.Unmarshal(data, j)
}
