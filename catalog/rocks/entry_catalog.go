package rocks

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
	CommonPrefix
	*Entry
}

// EntryCatalog
type EntryCatalog interface {
	RepositoryCatalog
	VersionControlCatalog

	// Get returns entry from repository / reference by path, nil entry is a valid entry for tombstone
	// returns error if entry does not exist
	GetEntry(ctx context.Context, repositoryID RepositoryID, ref Ref, path Path) (*Entry, error)

	// Set stores entry on repository / branch by path. nil entry is a valid entry for tombstone
	SetEntry(ctx context.Context, repositoryID RepositoryID, branchID BranchID, path Path, entry *Entry) error

	// DeleteEntry deletes entry from repository / branch by path
	DeleteEntry(ctx context.Context, repositoryID RepositoryID, branchID BranchID, path Path) error

	// List lists entries on repository / ref will filter by prefix, from path 'from'.
	//   When 'delimiter' is set the listing will include common prefixes based on the delimiter
	//   The 'amount' specifies the maximum amount of listing per call that the API will return (no more than ListEntriesMaxAmount, -1 will use the server default).
	//   Returns the list of entries, boolean specify if there are more results which will require another call with 'from' set to the last path from the previous call.
	ListEntries(ctx context.Context, repositoryID RepositoryID, ref Ref, prefix, from Path, delimiter string, amount int) ([]EntryListing, bool, error)
}

func NewPath(id string) (Path, error) {
	return Path(id), nil
}

func (id Path) String() string {
	return string(id)
}
