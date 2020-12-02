package rocks

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/treeverse/lakefs/ident"

	"github.com/treeverse/lakefs/catalog"

	"time"

	"github.com/treeverse/lakefs/uri"
)

// Basic Types

// DiffType represents a changed state for a given entry
type DiffType uint8

const (
	DiffTypeAdded DiffType = iota
	DiffTypeRemoved
	DiffTypeChanged
	DiffTypeConflict
)

// ReferenceType represents the type of the reference
type ReferenceType uint8

const (
	ReferenceTypeCommit ReferenceType = iota
	ReferenceTypeTag
	ReferenceTypeBranch
)

type Reference interface {
	Type() ReferenceType
	Branch() Branch
	CommitID() CommitID
}

// function/methods receiving the following basic types could assume they passed validation
type (
	// StorageNamespace is the URI to the storage location
	StorageNamespace string

	// RepositoryID is an identifier for a repo
	RepositoryID string

	// Path represents a logical path for an entry
	Path string

	// Ref could be a commit ID, a branch name, a Tag
	Ref string

	// TagID represents a named tag pointing at a commit
	TagID string

	// CommitID is a content addressable hash representing a Commit object
	CommitID string

	// CommitParents
	CommitParents []CommitID

	// BranchID is an identifier for a branch
	BranchID string

	// TreeID represents a snapshot of the tree, referenced by a commit
	TreeID string

	// StagingToken represents a namespace for writes to apply as uncommitted
	StagingToken string

	// CommonPrefix represents a path prefixing one or more Entry objects
	CommonPrefix string

	// Metadata key/value strings to holds metadata information on entry and commit
	Metadata map[string]string
)

// Repository represents repository metadata
type Repository struct {
	StorageNamespace StorageNamespace `db:"storage_namespace"`
	CreationDate     time.Time        `db:"creation_date"`
	DefaultBranchID  BranchID         `db:"default_branch"`
}

type RepositoryRecord struct {
	RepositoryID RepositoryID `db:"id"`
	*Repository
}

// Entry represents metadata or a given object (modified date, physical address, etc)
type Entry struct {
	LastModified time.Time
	Address      string
	Metadata     Metadata
	ETag         string
	Size         int64
}

// EntryRecord holds Path with the associated Entry information
type EntryRecord struct {
	Path Path
	*Entry
}

func (ps CommitParents) Identity() []byte {
	strings := make([]string, len(ps))
	for i, v := range ps {
		strings[i] = string(v)
	}
	buf := ident.NewBuffer()
	buf.MarshalStringList(strings)
	return buf.Identity()
}

// Commit represents commit metadata (author, time, tree ID)
type Commit struct {
	Committer    string           `db:"committer"`
	Message      string           `db:"message"`
	TreeID       TreeID           `db:"tree_id"`
	CreationDate time.Time        `db:"creation_date"`
	Parents      CommitParents    `db:"parents"`
	Metadata     catalog.Metadata `db:"metadata"`
}

func (c Commit) Identity() []byte {
	b := ident.NewBuffer()
	b.MarshalString("commit:v1")
	b.MarshalString(c.Committer)
	b.MarshalString(c.Message)
	b.MarshalString(string(c.TreeID))
	b.MarshalInt64(c.CreationDate.Unix())
	b.MarshalStringMap(c.Metadata)
	b.MarshalIdentifiable(c.Parents)
	return b.Identity()
}

// CommitRecords holds CommitID with the associated Commit data
type CommitRecord struct {
	CommitID CommitID `db:"id"`
	*Commit
}

// Branch is a pointer to a commit
type Branch struct {
	CommitID CommitID
	// nolint: structcheck, unused
	stagingToken StagingToken
}

// BranchRecord holds BranchID with the associated Branch data
type BranchRecord struct {
	BranchID BranchID
	*Branch
}

// Listing represents either an entry or a CommonPrefix
type Listing struct {
	CommonPrefix
	*Entry
}

// Diff represents a change in path
type Diff struct {
	Path Path
	Type DiffType
}

// Interfaces
type Catalog interface {
	// GetRepository returns the Repository metadata object for the given RepositoryID
	GetRepository(ctx context.Context, repositoryID RepositoryID) (*Repository, error)

	// CreateRepository stores a new Repository under RepositoryID with the given Branch as default branch
	CreateRepository(ctx context.Context, repositoryID RepositoryID, stoargeNamespace StorageNamespace, branchID BranchID) (*Repository, error)

	// ListRepositories lists repositories starting with 'from' returns maximum 'amount' results and true (boolean)
	// in case more results exist
	ListRepositories(ctx context.Context, from RepositoryID, amount int) ([]RepositoryRecord, bool, error)

	// DeleteRepository deletes the repository
	DeleteRepository(ctx context.Context, repositoryID RepositoryID) error

	// GetEntry returns entry from repository / reference by path, nil entry is a valid value for tombstone
	// returns error if entry does not exist
	GetEntry(ctx context.Context, repositoryID RepositoryID, ref Ref, path Path) (*Entry, error)

	// SetEntry stores entry on repository / branch by path. nil entry is a valid value for tombstone
	SetEntry(ctx context.Context, repositoryID RepositoryID, branchID BranchID, path Path, entry Entry) error

	// DeleteEntry deletes entry on repository / branch by path
	DeleteEntry(ctx context.Context, repositoryID RepositoryID, branchID BranchID, path Path) error

	// ListEntries lists entries on repository / ref will filter by prefix, from path 'from'.
	//   When 'delimiter' is set the listing will include common prefixes based on the delimiter
	//   The 'amount' specifies the maximum amount of listing per call that the API will return (no more than ListEntriesMaxAmount, -1 will use the server default).
	//   Returns the list of entries, boolean specify if there are more results which will require another call with 'from' set to the last path from the previous call.
	ListEntries(ctx context.Context, repositoryID RepositoryID, ref Ref, prefix, from, delimiter string, amount int) ([]Listing, bool, error)

	// CreateBranch creates branch on repository pointing to ref
	CreateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (Branch, error)

	// UpdateBranch updates branch on repository pointing to ref
	UpdateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (Branch, error)

	// GetBranch gets branch information by branch / repository id
	GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (Branch, error)

	// Log lists commits in repository
	//   The 'from' is used to get all commits after the specified commit id
	//   The 'amount' specifies the maximum number of commits the call will return
	// 	 Returns commits, has more boolean and an error
	Log(ctx context.Context, repositoryID RepositoryID, from CommitID, amount int) ([]Commit, bool, error)

	// ListBranches lists branches on repositories
	//   The 'from' is used to get all branches after this branch id
	//   The 'amount' specifies the maximum number of branches the call will return
	//   Returns branches, has more boolean and an error
	ListBranches(ctx context.Context, repositoryID RepositoryID, from BranchID, amount int) ([]Branch, bool, error)

	// DeleteBranch deletes branch from repository
	DeleteBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error

	// Commit the staged data and returns a commit ID that references that change
	//   ErrNothingToCommit in case there is no data in stage
	Commit(ctx context.Context, repositoryID RepositoryID, branchID BranchID, committer string, message string, metadata Metadata) (CommitID, error)

	// GetCommit returns the Commit metadata object for the given CommitID
	GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error)

	// Dereference returns the commit ID based on 'ref' reference
	Dereference(ctx context.Context, repositoryID RepositoryID, ref Ref) (CommitID, error)

	// Reset throw all staged data on the repository / branch
	Reset(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error

	// Revert commits a change that will revert all the changes make from 'ref' specified
	Revert(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (CommitID, error)

	// Merge merge 'from' with 'to' branches under repository returns the new commit id on 'to' branch
	Merge(ctx context.Context, repositoryID RepositoryID, from Ref, to BranchID) (CommitID, error)

	// DiffUncommitted returns the changes as 'Diff' slice on a repository / branch
	//   List the differences 'from' path, with 'amount' of result.
	//   Returns differences found, true (boolean) in case there are more differences - use 'from' with last path from previous call to get the next differences
	DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID, from Path, amount int) ([]Diff, bool, error)

	// Diff returns the changes between 'left' and 'right' ref, list changes 'from' path with no more than 'amount' per call.
	//   Returns the list of changes, true (boolean) in case there are more differences - use last path as 'from' in the next call to continue getting differences
	Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref, from Path, amount int) ([]Diff, bool, error)
}

// Internal structures used by Catalog
// xxxIterator used as follow:
// ```
// it := NewXXXIterator(data)
// for it.Next() {
//    data := it.Value()
//    process(data)
// }
// if it.Err() {
//   return fmt.Errorf("stopped because of an error %w", it.Err())
// }
// ```
// Calling SeekGE() returns true, like calling Next() - we can process 'Value()' when true and check Err() in case of false
// When Next() or SeekGE() returns false (doesn't matter if it because of an error) calling Value() should return nil

type RepositoryIterator interface {
	Next() bool
	SeekGE(id RepositoryID) bool
	Value() *RepositoryRecord
	Err() error
	Close()
}

type EntryIterator interface {
	Next() bool
	SeekGE(id Path) bool
	Value() *EntryRecord
	Err() error
	Close()
}

type DiffIterator interface {
	Next() bool
	SeekGE(id Path) bool
	Value() *Diff
	Err() error
	Close()
}

type BranchIterator interface {
	Next() bool
	SeekGE(id BranchID) bool
	Value() *BranchRecord
	Err() error
	Close()
}

type CommitIterator interface {
	Next() bool
	SeekGE(id CommitID) bool
	Value() *CommitRecord
	Err() error
	Close()
}

// These are the more complex internal components that compose the functionality of the Catalog

// RefManager handles references: branches, commits, probably tags in the future
// it also handles the structure of the commit graph and its traversal (notably, merge-base and log)
type RefManager interface {
	// GetRepository returns the Repository metadata object for the given RepositoryID
	GetRepository(ctx context.Context, repositoryID RepositoryID) (*Repository, error)

	// CreateRepository stores a new Repository under RepositoryID with the given Branch as default branch
	CreateRepository(ctx context.Context, repositoryID RepositoryID, repository Repository, branch Branch) error

	// ListRepositories lists repositories
	ListRepositories(ctx context.Context, from RepositoryID) (RepositoryIterator, error)

	// DeleteRepository deletes the repository
	DeleteRepository(ctx context.Context, repositoryID RepositoryID) error

	// RevParse returns the Reference matching the given Ref
	RevParse(ctx context.Context, repositoryID RepositoryID, ref Ref) (Reference, error)

	// GetBranch returns the Branch metadata object for the given BranchID
	GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*Branch, error)

	// SetBranch points the given BranchID at the given Branch metadata
	SetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, branch Branch) error

	// DeleteBranch deletes the branch
	DeleteBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error

	// ListBranches lists branches
	ListBranches(ctx context.Context, repositoryID RepositoryID, from BranchID) (BranchIterator, error)

	// GetCommit returns the Commit metadata object for the given CommitID.
	// The given CommitID may be arbitrarily truncated and should return a valid commit as long
	// as results are not ambiguous
	GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error)

	// AddCommit stores the Commit object, returning its ID
	AddCommit(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error)

	// FindMergeBase returns the merge-base for the given CommitIDs
	// see: https://git-scm.com/docs/git-merge-base
	// and internally: https://github.com/treeverse/lakeFS/blob/09954804baeb36ada74fa17d8fdc13a38552394e/index/dag/commits.go
	FindMergeBase(ctx context.Context, repositoryID RepositoryID, commitIDs ...CommitID) (*Commit, error)

	// Log returns an iterator that reads all parents up to the first commit
	Log(ctx context.Context, repositoryID RepositoryID, from CommitID) (CommitIterator, error)
}

// CommittedManager reads and applies committed snapshots
// it is responsible for de-duping them, persisting them and providing basic diff, merge and list capabilities
type CommittedManager interface {
	// GetEntry returns the provided path, if exists, from the provided TreeID
	GetEntry(ctx context.Context, ns StorageNamespace, treeID TreeID, path Path) (*Entry, error)

	// ListEntries takes a given tree and returns an EntryIterator seeked to >= "from" path
	ListEntries(ctx context.Context, ns StorageNamespace, treeID TreeID, from Path) (EntryIterator, error)

	// Diff receives two trees and a 3rd merge base tree used to resolve the change type
	// it tracks changes from left to right, returning an iterator of Diff entries
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
	// GetEntry returns the provided path (or nil entry to represent a tombstone)
	//   Returns ErrNotFound if no entry found on path
	GetEntry(ctx context.Context, repositoryID RepositoryID, branchID BranchID, st StagingToken, from Path) (*Entry, error)

	// SetEntry writes an entry (or nil entry to represent a tombstone)
	SetEntry(ctx context.Context, repositoryID RepositoryID, branchID BranchID, path Path, entry *Entry) error

	// DeleteEntry deletes an entry by path
	DeleteEntry(ctx context.Context, repositoryID RepositoryID, branchID BranchID, path Path) error

	// ListEntries takes a given BranchID and returns an EntryIterator seeked to >= "from" path
	ListEntries(ctx context.Context, repositoryID RepositoryID, branchID BranchID, st StagingToken, from Path) (EntryIterator, error)

	// Snapshot returns a new snapshot and returns it's ID
	Snapshot(ctx context.Context, repositoryID RepositoryID, branchID BranchID, st StagingToken) (StagingToken, error)

	// ListSnapshot returns an iterator to scan the snapshot entries
	ListSnapshot(ctx context.Context, repositoryID RepositoryID, branchID BranchID, st StagingToken, from Path) (EntryIterator, error)
}

var (
	reValidBranchID     = regexp.MustCompile(`^\w[-\w]*$`)
	reValidRepositoryID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
)

// Catalog errors
var (
	ErrNotFound                = errors.New("not found")
	ErrInvalidValue            = errors.New("invalid value")
	ErrInvalidStorageNamespace = fmt.Errorf("storage namespace %w", ErrInvalidValue)
	ErrInvalidRepositoryID     = fmt.Errorf("repository id %w", ErrInvalidValue)
	ErrInvalidBranchID         = fmt.Errorf("branch id %w", ErrInvalidValue)
	ErrInvalidRef              = fmt.Errorf("ref %w", ErrInvalidValue)
	ErrInvalidCommitID         = fmt.Errorf("commit id %w", ErrInvalidValue)
	ErrCommitNotFound          = fmt.Errorf("commit %w", ErrNotFound)
)

func NewRepositoryID(id string) (RepositoryID, error) {
	if !reValidRepositoryID.MatchString(id) {
		return "", ErrInvalidRepositoryID
	}
	return RepositoryID(id), nil
}

func (id RepositoryID) String() string {
	return string(id)
}

func NewStorageNamespace(ns string) (StorageNamespace, error) {
	u, err := uri.Parse(ns)
	if err != nil || u.Protocol == "" {
		return "", ErrInvalidStorageNamespace
	}
	return StorageNamespace(ns), nil
}

func (ns StorageNamespace) String() string {
	return string(ns)
}

func NewBranchID(id string) (BranchID, error) {
	if !reValidBranchID.MatchString(id) {
		return "", ErrInvalidBranchID
	}
	return BranchID(id), nil
}

func (id BranchID) String() string {
	return string(id)
}

func NewRef(id string) (Ref, error) {
	if id == "" || strings.ContainsAny(id, " \t\r\n") {
		return "", ErrInvalidRef
	}
	return Ref(id), nil
}

func (id Ref) String() string {
	return string(id)
}

func NewPath(id string) (Path, error) {
	return Path(id), nil
}

func (id Path) String() string {
	return string(id)
}

func NewCommitID(id string) (CommitID, error) {
	_, err := hex.DecodeString(id)
	if err != nil {
		return "", ErrInvalidCommitID
	}
	return CommitID(id), nil
}

func (id CommitID) String() string {
	return string(id)
}
