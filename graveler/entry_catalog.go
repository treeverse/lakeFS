package graveler

import (
	"context"
	"time"
)

type Path string

// Entry represents metadata or a given object (modified date, physical address, etc)
type Entry struct {
	LastModified time.Time
	Address      string
	Metadata     Metadata
	ETag         string
	Size         int64
}

type EntryRecord struct {
	Path Path
	*Entry
}

type EntryListing struct {
	CommonPrefix bool
	Path
	*Entry
}

type EntryListingIterator interface {
	Next() bool
	SeekGE(id Path) bool
	Value() *EntryListing
	Err() error
	Close()
}

// EntryCatalog
type EntryCatalog interface {
	VersionController

	// Get returns entry from repository / reference by path, nil entry is a valid entry for tombstone
	// returns error if entry does not exist
	GetEntry(ctx context.Context, repositoryID RepositoryID, ref Ref, path Path) (*Entry, error)

	// Set stores entry on repository / branch by path. nil entry is a valid entry for tombstone
	SetEntry(ctx context.Context, repositoryID RepositoryID, branchID BranchID, path Path, entry *Entry) error

	// DeleteEntry deletes entry from repository / branch by path
	DeleteEntry(ctx context.Context, repositoryID RepositoryID, branchID BranchID, path Path) error

	// List lists entries on repository / ref will filter by prefix, from path 'from'.
	//   When 'delimiter' is set the listing will include common prefixes based on the delimiter
	ListEntries(ctx context.Context, repositoryID RepositoryID, ref Ref, prefix, from, delimiter Path) (EntryListingIterator, error)
}

func NewPath(id string) (Path, error) {
	return Path(id), nil
}

func (id Path) String() string {
	return string(id)
}
