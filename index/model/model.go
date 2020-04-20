package model

import (
	"database/sql/driver"
	"encoding/json"
	"strconv"
	"time"
)

type JSONMetadata map[string]string

func (j JSONMetadata) Value() (driver.Value, error) {
	// marshal to json
	if j == nil {
		return json.Marshal(map[string]string{})
	}
	return json.Marshal(j)
}

func (j *JSONMetadata) Scan(src interface{}) error {
	// read bytes from db and hydrate j
	if src != nil {
		return json.Unmarshal(src.([]byte), j)
	}
	return nil
}

type JSONStringSlice []string

func (j JSONStringSlice) Value() (driver.Value, error) {
	// marshal to json
	if j == nil {
		return json.Marshal([]string{})
	}
	return json.Marshal(j)
}

func (j *JSONStringSlice) Scan(src interface{}) error {
	// read bytes from db and hydrate j
	if src != nil {
		return json.Unmarshal(src.([]byte), j)
	}
	return nil
}

type Block struct {
	Address string `json:"address"`
	Size    int64  `json:"size"`
}

type JSONBlocks []*Block

func (j JSONBlocks) Value() (driver.Value, error) {
	// marshal to json
	if j == nil {
		return json.Marshal([]*Block{})
	}
	return json.Marshal(j)
}

func (j *JSONBlocks) Scan(src interface{}) error {
	if src != nil {
		return json.Unmarshal(src.([]byte), j)
	}
	return nil
}

type Repo struct {
	Id               string    `db:"id"`
	StorageNamespace string    `db:"storage_namespace"`
	CreationDate     time.Time `db:"creation_date"`
	DefaultBranch    string    `db:"default_branch"`
}

type Object struct {
	RepositoryId string       `db:"repository_id"`
	Address      string       `db:"address"`
	Checksum     string       `db:"checksum"`
	Size         int64        `db:"size"`
	Blocks       JSONBlocks   `db:"blocks"`
	Metadata     JSONMetadata `db:"metadata"`
}

func (m *Object) Identity() []byte {
	blocks := m.Blocks
	addresses := make([]string, len(blocks))
	for i, block := range blocks {
		addresses[i] = block.Address
	}
	return append(
		identFromStrings(addresses...),
		identFromStrings(
			identMapToString(m.Metadata),
		)...,
	)
}

type ObjectDedup struct {
	RepositoryId string `db:"repository_id"`
	DedupId      string `db:"dedup_id"`
	Address      string `db:"address"`
}

type Root struct {
	RepositoryId string    `db:"repository_id"`
	Address      string    `db:"address"`
	CreationDate time.Time `db:"creation_date"`
	Size         int64     `db:"size"`
}

const (
	EntryTypeObject = "object"
	EntryTypeTree   = "tree"
)

type Entry struct {
	RepositoryId  string    `db:"repository_id"`
	ParentAddress string    `db:"parent_address"`
	Name          string    `db:"name"`
	Address       string    `db:"address"`
	EntryType     string    `db:"type"`
	CreationDate  time.Time `db:"creation_date"`
	Size          int64     `db:"size"`
	Checksum      string    `db:"checksum"`
}

func (e *Entry) GetName() string {
	return e.Name
}

func (e *Entry) GetType() string {
	return e.EntryType
}

func (e *Entry) GetAddress() string {
	return e.Address
}

func (e *Entry) Identity() []byte {
	return identFromStrings(
		e.Name,
		e.Address,
		e.EntryType)
}

type Commit struct {
	RepositoryId string          `db:"repository_id"`
	Address      string          `db:"address"`
	Tree         string          `db:"tree"`
	Committer    string          `db:"committer"`
	Message      string          `db:"message"`
	CreationDate time.Time       `db:"creation_date"`
	Parents      JSONStringSlice `db:"parents"`
	Metadata     JSONMetadata    `db:"metadata"`
}

func (c *Commit) Identity() []byte {
	return append(identFromStrings(
		c.Tree,
		c.Committer,
		c.Message,
		strconv.FormatInt(c.CreationDate.Unix(), 10),
		identMapToString(c.Metadata),
	), identFromStrings(c.Parents...)...)
}

type Branch struct {
	RepositoryId  string `db:"repository_id"`
	Id            string `db:"id"`
	CommitId      string `db:"commit_id"`
	CommitRoot    string `db:"commit_root"`
	WorkspaceRoot string `db:"workspace_root"`
}

type WorkspaceEntry struct {
	RepositoryId string `db:"repository_id"`
	BranchId     string `db:"branch_id"`
	ParentPath   string `db:"parent_path"`
	Path         string `db:"path"`

	EntryName         *string    `db:"entry_name"`
	EntryAddress      *string    `db:"entry_address"`
	EntryType         *string    `db:"entry_type"`
	EntryCreationDate *time.Time `db:"entry_creation_date"`
	EntrySize         *int64     `db:"entry_size"`
	EntryChecksum     *string    `db:"entry_checksum"`

	Tombstone bool `db:"tombstone"`
}

func (ws *WorkspaceEntry) GetName() string {
	return *ws.EntryName
}

func (ws *WorkspaceEntry) GetType() string {
	return *ws.EntryType
}

func (ws *WorkspaceEntry) GetAddress() string {
	return *ws.EntryAddress
}

func dstr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func dint64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func dtime(p *time.Time) time.Time {
	if p == nil {
		var t time.Time
		return t
	}
	return *p
}

func (ws *WorkspaceEntry) Entry() *Entry {
	return &Entry{
		RepositoryId: ws.RepositoryId,
		Name:         dstr(ws.EntryName),
		Address:      dstr(ws.EntryAddress),
		EntryType:    dstr(ws.EntryType),
		CreationDate: dtime(ws.EntryCreationDate),
		Size:         dint64(ws.EntrySize),
		Checksum:     dstr(ws.EntryChecksum),
	}
}
