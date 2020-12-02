package catalog

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

const (
	DBEntryFieldChecksum        = "checksum"
	DBEntryFieldPhysicalAddress = "physical_address"
)

type Metadata map[string]string

type Repository struct {
	Name             string    `db:"name"`
	StorageNamespace string    `db:"storage_namespace"`
	DefaultBranch    string    `db:"default_branch"`
	CreationDate     time.Time `db:"creation_date"`
}

type Entry struct {
	CommonLevel     bool
	Path            string    `db:"path"`
	PhysicalAddress string    `db:"physical_address"`
	CreationDate    time.Time `db:"creation_date"`
	Size            int64     `db:"size"`
	Checksum        string    `db:"checksum"`
	Metadata        Metadata  `db:"metadata"`
	Expired         bool      `db:"is_expired"`
}

type CommitLog struct {
	Reference    string
	Committer    string    `db:"committer"`
	Message      string    `db:"message"`
	CreationDate time.Time `db:"creation_date"`
	Metadata     Metadata  `db:"metadata"`
	Parents      []string
}

type MergeResult struct {
	Summary   map[DifferenceType]int
	Reference string
}

type Branch struct {
	Repository string `db:"repository"`
	Name       string `db:"name"`
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
		return ErrInvalidMetadataSrcFormat
	}
	return json.Unmarshal(data, j)
}
