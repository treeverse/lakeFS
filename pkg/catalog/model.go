package catalog

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/treeverse/lakefs/pkg/block"
)

const (
	DefaultContentType = "application/octet-stream"
)

type Metadata map[string]string

type Repository struct {
	Name             string
	StorageNamespace string
	DefaultBranch    string
	CreationDate     time.Time
}

type DBEntry struct {
	CommonLevel     bool
	Path            string
	PhysicalAddress string
	CreationDate    time.Time
	Size            int64
	Checksum        string
	Metadata        Metadata
	Expired         bool
	AddressType     AddressType
	ContentType     string
}

type CommitLog struct {
	Reference    string
	Committer    string
	Message      string
	CreationDate time.Time
	Metadata     Metadata
	MetaRangeID  string
	Parents      []string
}

type Branch struct {
	Name      string
	Reference string
}

type Tag struct {
	ID       string
	CommitID string
}

// AddressType is the type of an entry address
type AddressType int32

const (
	// Deprecated: indicates that the address might be relative or full.
	// Used only for backward compatibility and should not be used for creating entries.
	AddressTypeByPrefixDeprecated AddressType = 0

	// AddressTypeRelative indicates that the address is relative to the storage namespace.
	// For example: "foo/bar"
	AddressTypeRelative AddressType = 1

	// AddressTypeFull indicates that the address is the full address of the object in the object store.
	// For example: "s3://bucket/foo/bar"
	AddressTypeFull AddressType = 2
)

//nolint:staticcheck
func (at AddressType) ToIdentifierType() block.IdentifierType {
	switch at {
	case AddressTypeByPrefixDeprecated:
		return block.IdentifierTypeUnknownDeprecated
	case AddressTypeRelative:
		return block.IdentifierTypeRelative
	case AddressTypeFull:
		return block.IdentifierTypeFull
	default:
		panic(fmt.Sprintf("unknown address type: %d", at))
	}
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

func ContentTypeOrDefault(ct string) string {
	if ct == "" {
		return DefaultContentType
	}
	return ct
}

// DBEntryBuilder DBEntry builder
type DBEntryBuilder struct {
	dbEntry DBEntry
}

func NewDBEntryBuilder() *DBEntryBuilder {
	return &DBEntryBuilder{}
}

func (b *DBEntryBuilder) CommonLevel(commonLevel bool) *DBEntryBuilder {
	b.dbEntry.CommonLevel = commonLevel
	return b
}

func (b *DBEntryBuilder) Path(path string) *DBEntryBuilder {
	b.dbEntry.Path = path
	return b
}

func (b *DBEntryBuilder) RelativeAddress(relative bool) *DBEntryBuilder {
	if relative {
		b.dbEntry.AddressType = AddressTypeRelative
	} else {
		b.dbEntry.AddressType = AddressTypeFull
	}
	return b
}

func (b *DBEntryBuilder) PhysicalAddress(physicalAddress string) *DBEntryBuilder {
	b.dbEntry.PhysicalAddress = physicalAddress
	return b
}

func (b *DBEntryBuilder) CreationDate(creationDate time.Time) *DBEntryBuilder {
	b.dbEntry.CreationDate = creationDate
	return b
}

func (b *DBEntryBuilder) Size(size int64) *DBEntryBuilder {
	b.dbEntry.Size = size
	return b
}

func (b *DBEntryBuilder) Checksum(checksum string) *DBEntryBuilder {
	b.dbEntry.Checksum = checksum
	return b
}

func (b *DBEntryBuilder) Metadata(metadata Metadata) *DBEntryBuilder {
	b.dbEntry.Metadata = metadata
	return b
}

func (b *DBEntryBuilder) Expired(expired bool) *DBEntryBuilder {
	b.dbEntry.Expired = expired
	return b
}

func (b *DBEntryBuilder) AddressType(addressType AddressType) *DBEntryBuilder {
	b.dbEntry.AddressType = addressType
	return b
}

func (b *DBEntryBuilder) ContentType(contentType string) *DBEntryBuilder {
	b.dbEntry.ContentType = contentType
	return b
}

func (b *DBEntryBuilder) Build() DBEntry {
	if !b.dbEntry.CommonLevel && b.dbEntry.ContentType == "" {
		b.dbEntry.ContentType = DefaultContentType
	}
	return b.dbEntry
}
