package rocks3

import (
	"context"
	"encoding/hex"
	"io"
	"time"
)

// Basic Types

// Repository represents repository metadata
type Repository struct {
	StorageNamespace StorageNamespace
	CreationDate     int64
	DefaultBranch    string
}

// Entry represents metadata or a given object (modified date, physical address, etc)
type Entry struct {
	LastModified    time.Time
	PhysicalAddress string
	Metadata        map[string]string
	ETag            []byte
}

// Commit represents commit metadata (author, time, tree ID)
type Commit struct {
	Author   string
	TreeID   TreeID
	Created  time.Time
	Parents  []CommitID
	Metadata map[string]string
}

// Branch is a pointer to a commit.
type Branch struct {
	CommitID     CommitID
	stagingToken StagingToken
}

// Diff represents a changed state for a given entry (added, removed, changed, conflict)
type DiffType uint8

const (
	DiffTypeAdded DiffType = iota
	DiffTypeRemoved
	DiffTypeChanged
	DiffTypeConflict
)

type Diff struct {
	Path Path
	Type DiffType
}

type Hash [32]byte

func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

type StorageNamespace string

// RepositoryID is an identifier for a repo
type RepositoryID string

// Path represents a logical path for an entry
type Path string

// Ref could be a commit ID, a branch name, a Tag
type Ref string

// TagID represents a named tag pointing at a commit
type TagID string

// CommitID is a content addressable hash representing a Commit object
type CommitID Hash

// BranchID is an identifier for a branch
type BranchID string

// TreeID represents a snapshot of the tree, referenced by a commit
type TreeID Hash

// StagingToken represents a namespace for writes to apply as uncommitted
type StagingToken string

// CommonPrefix represents a path prefixing one or more Entry objects
type CommonPrefix string

// Listing represents either an entry or a CommonPrefix
type Listing struct {
	CommonPrefix
	*Entry
}

// Interfaces
type Catalog interface {
	// entries
	GetEntry(RepositoryID, Ref, Path) (*Entry, error)
	SetEntry(RepositoryID, BranchID, Path, Entry) error
	DeleteEntry(RepositoryID, BranchID, Path) error
	ListEntries(repo RepositoryID, ref Ref, prefix, from, delimiter string, amount int) ([]Listing, bool, error)

	// refs
	CreateBranch(RepositoryID, BranchID, Ref) (Branch, error)
	GetBranch(RepositoryID, BranchID) (Branch, error)
	Dereference(RepositoryID, Ref) (CommitID, error)
	ListLog(repo RepositoryID, commitID CommitID, amount int) ([]Commit, bool, error)
	ListBranches(repo RepositoryID, from BranchID, amount int) ([]Branch, bool, error)
	DeleteBranch(RepositoryID, BranchID) error

	// commits
	Commit(RepositoryID, BranchID, Commit) (CommitID, error)
	Reset(RepositoryID, BranchID) error
	Revert(RepositoryID, BranchID, Ref) error

	// diffs and merges
	Merge(repo RepositoryID, from Ref, to BranchID) (CommitID, error)
	DiffUncommitted(repo RepositoryID, branch BranchID, from Path, amount int) ([]Diff, bool, error)
	Diff(repo RepositoryID, left, right Ref, from Path, amount int) ([]Diff, bool, error)
}

// internal structures used by Catalog
type EntryIterator interface {
	First() (*Path, *Entry)
	SeekGE(Path) (*Path, *Entry)
	Next() (*Path, *Entry)
	io.Closer
}

type DiffIterator interface {
	First() (*Path, *DiffType)
	SeekGE(Path) (*Path, *DiffType)
	Next() (*Path, *DiffType)
	io.Closer
}

type BranchIterator interface {
	First() (*BranchID, *Branch)
	Next() (*BranchID, *Branch)
	SeekGE(BranchID) (*BranchID, *Branch)
	io.Closer
}

type CommitIterator interface {
	First() (*CommitID, *Commit)
	Next() (*CommitID, *Commit)
	io.Closer
}

// These are the more complex internal components that compose the functionality of the Catalog

// RefManager handles references: branches, commits, probably tags in the future
// it also handles the structure of the commit graph and its traversal (notably, merge-base and log)
type RefManager interface {
	// GetRepository returns the Repository metadata object for the given RepositoryID
	GetRepository(RepositoryID) (*Repository, error)

	// SetRepository points the given RepositoryID at the given Repository
	SetRepository(RepositoryID, Repository) error

	// Dereference takes a Ref and translates it to the corresponding CommitID
	Dereference(RepositoryID, Ref) (CommitID, error)

	// GetBranch returns the Branch metadata object for the given BranchID
	GetBranch(RepositoryID, BranchID) (*Commit, error)

	// SetBranch points the given BranchID at the given Branch metadata
	SetBranch(RepositoryID, BranchID, Branch) error

	// GetCommit returns the Commit metadata object for the given CommitID
	GetCommit(RepositoryID, CommitID) (*Commit, error)

	// SetCommit stores the Commit object, returning its ID
	SetCommit(RepositoryID, Commit) (CommitID, error)

	// ListBranches lists branches
	ListBranches(repo RepositoryID, from BranchID) (BranchIterator, error)

	// FindMergeBase returns the merge-base for the given CommitIDs
	// see: https://git-scm.com/docs/git-merge-base
	// and internally: https://github.com/treeverse/lakeFS/blob/09954804baeb36ada74fa17d8fdc13a38552394e/index/dag/commits.go
	FindMergeBase(RepositoryID, ...CommitID) (*Commit, error)

	// Log returns an iterator that reads all parents up to the first commit
	Log(RepositoryID, CommitID) (CommitIterator, error)
}

// CommittedManager reads and applies committed snapshots
// it is responsible for de-duping them, persisting them and providing basic diff, merge and list capabilities
type CommittedManager interface {
	// GetEntry returns the provided path, if exists, from the provided TreeID
	GetEntry(ctx context.Context, ns StorageNamespace, treeID TreeID, path Path) (*Entry, error)

	// ListEntries takes a given tree and returns an EntryIterator seeked to >= "from" path
	ListEntries(ctx context.Context, ns StorageNamespace, treeID TreeID, from Path) (EntryIterator, error)

	// Diff receives two trees and a 3rd merge base tree used to resolve the change type
	//it tracks changes from left to right, returning an iterator of Diff entries
	Diff(ctx context.Context, ns StorageNamespace, left, right, base TreeID, from Path) (DiffIterator, error)

	// Merge receives two trees and a 3rd merge base tree used to resolve the change type
	// it applies that changes from left to right, resulting in a new tree that
	// is expected to be immediately addressable
	Merge(ctx context.Context, ns StorageNamespace, left, right, base TreeID) (TreeID, error)

	// Apply is the act of taking an existing tree (snapshot) and applying a set of changes to it.
	// A change is either an entity to write/overwrite, or a tombstone to mark a deletion
	// it returns a new treeID that is expected to be immediately addressable
	Apply(ctx context.Context, ns StorageNamespace, treeID TreeID, entryIterator EntryIterator) (TreeID, error)
}

// StagingManager handles changes to a branch that aren't yet committed
// provides basic CRUD abilities, with deletes being written as tombstones (null entry)
type StagingManager interface {
	// GetEntry returns the provided path, if exists, for the given StagingToken
	GetEntry(sit StagingToken, from Path) (*Entry, error)

	// ListEntries takes a given BranchID and returns an EntryIterator seeked to >= "from" path
	ListEntries(sid StagingToken, from Path) (EntryIterator, error)

	// SetEntry writes an entry (or null entry to represent a tombstone)
	SetEntry(sid StagingToken, path Path, entry *Entry) error

	// DropStaging deletes all entries and tombstones for a given StagingToken
	// This is useful in a `lakefs reset` operation, and potentially as a last step of a commit
	DropStaging(sid StagingToken) error
}
