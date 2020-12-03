package graveler

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Basic Types

// DiffType represents the type of the change
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

// StorageNamespace is the URI to the storage location
type StorageNamespace string

// RepositoryID is an identifier for a repo
type RepositoryID string

// Key represents a logical path for an value
type Key []byte

// Ref could be a commit ID, a branch name, a Tag
type Ref string

// TagID represents a named tag pointing at a commit
type TagID string

// CommitID is a content addressable hash representing a Commit object
type CommitID string

// BranchID is an identifier for a branch
type BranchID string

// TreeID represents a snapshot of the tree, referenced by a commit
type TreeID string

// StagingToken represents a namespace for writes to apply as uncommitted
type StagingToken string

// CommonPrefix represents one or more keys with the same key prefix
type CommonPrefix []byte

// Metadata key/value strings to holds metadata information on value and commit
type Metadata map[string]string

// Repository represents repository metadata
type Repository struct {
	StorageNamespace StorageNamespace
	CreationDate     time.Time
	DefaultBranchID  BranchID
}

type RepositoryRecord struct {
	RepositoryID RepositoryID
	*Repository
}

// Value represents metadata or a given object (modified date, physical address, etc)
type Value struct {
	Identity []byte `db:"identity"`
	Data     []byte `db:"data"`
}

// ValueRecord holds Key with the associated Value information
type ValueRecord struct {
	Key Key `db:"key"`
	*Value
}

// Commit represents commit metadata (author, time, tree ID)
type Commit struct {
	Committer    string
	Message      string
	TreeID       TreeID
	CreationDate time.Time
	Parents      []CommitID
	Metadata     map[string]string
}

// CommitRecords holds CommitID with the associated Commit data
type CommitRecord struct {
	CommitID CommitID
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

// Listing of key/value when common prefix is true, value is nil
type Listing struct {
	CommonPrefix bool
	Key
	*Value
}

// Diff represents a change in value based on key
type Diff struct {
	Type  DiffType
	Key   Key
	Value *Value
}

// Interfaces

type KeyValueStore interface {
	// Get returns value from repository / reference by key, nil value is a valid value for tombstone
	// returns error if value does not exist
	Get(ctx context.Context, repositoryID RepositoryID, ref Ref, key Key) (*Value, error)

	// Set stores value on repository / branch by key. nil value is a valid value for tombstone
	Set(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key, value Value) error

	// Delete value from repository / branch branch by key
	Delete(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error

	// List lists entries on repository / ref will filter by prefix, from key 'from'.
	//   When 'delimiter' is set the listing will include common prefixes based on the delimiter
	List(ctx context.Context, repositoryID RepositoryID, ref Ref, prefix, from, delimiter Key) (ListingIterator, error)
}

type VersionController interface {
	// GetRepository returns the Repository metadata object for the given RepositoryID
	GetRepository(ctx context.Context, repositoryID RepositoryID) (*Repository, error)

	// CreateRepository stores a new Repository under RepositoryID with the given Branch as default branch
	CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*Repository, error)

	// ListRepositories returns iterator to scan repositories
	ListRepositories(ctx context.Context, from RepositoryID) (RepositoryIterator, error)

	// DeleteRepository deletes the repository
	DeleteRepository(ctx context.Context, repositoryID RepositoryID) error

	// CreateBranch creates branch on repository pointing to ref
	CreateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (Branch, error)

	// UpdateBranch updates branch on repository pointing to ref
	UpdateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (Branch, error)

	// GetBranch gets branch information by branch / repository id
	GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (Branch, error)

	// Log returns an iterator starting at commit ID up to repository root
	Log(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (CommitIterator, error)

	// ListBranches lists branches on repositories
	ListBranches(ctx context.Context, repositoryID RepositoryID) (BranchIterator, error)

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

	// DiffUncommitted returns iterator to scan the changes made on the branch
	DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (DiffIterator, error)

	// Diff returns the changes between 'left' and 'right' ref, starting from the 'from' key
	Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref, from Key) (DiffIterator, error)
}

type Graveler interface {
	KeyValueStore
	VersionController
}

// Internal structures used by Graveler
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

type ValueIterator interface {
	Next() bool
	SeekGE(id Key) bool
	Value() *ValueRecord
	Err() error
	Close()
}

type DiffIterator interface {
	Next() bool
	SeekGE(id Key) bool
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

type ListingIterator interface {
	Next() bool
	SeekGE(id Key) bool
	Value() *Listing
	Err() error
	Close()
}

// These are the more complex internal components that compose the functionality of the Graveler

// RefManager handles references: branches, commits, probably tags in the future
// it also handles the structure of the commit graph and its traversal (notably, merge-base and log)
type RefManager interface {
	// GetRepository returns the Repository metadata object for the given RepositoryID
	GetRepository(ctx context.Context, repositoryID RepositoryID) (*Repository, error)

	// CreateRepository stores a new Repository under RepositoryID with the given Branch as default branch
	CreateRepository(ctx context.Context, repositoryID RepositoryID, repository Repository, branch Branch) error

	// ListRepositories lists repositories
	ListRepositories(ctx context.Context) (RepositoryIterator, error)

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
	ListBranches(ctx context.Context, repositoryID RepositoryID) (BranchIterator, error)

	// GetCommit returns the Commit metadata object for the given CommitID
	GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error)

	// AddCommit stores the Commit object, returning its ID
	AddCommit(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error)

	// FindMergeBase returns the merge-base for the given CommitIDs
	// see: https://git-scm.com/docs/git-merge-base
	// and internally: https://github.com/treeverse/lakeFS/blob/09954804baeb36ada74fa17d8fdc13a38552394e/index/dag/commits.go
	FindMergeBase(ctx context.Context, repositoryID RepositoryID, commitIDs ...CommitID) (*Commit, error)

	// Log returns an iterator starting at commit ID up to repository root
	Log(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (CommitIterator, error)
}

// CommittedManager reads and applies committed snapshots
// it is responsible for de-duping them, persisting them and providing basic diff, merge and list capabilities
type CommittedManager interface {
	// Get returns the provided key, if exists, from the provided TreeID
	Get(ctx context.Context, ns StorageNamespace, treeID TreeID, key Key) (*Value, error)

	// List takes a given tree and returns an ValueIterator
	List(ctx context.Context, ns StorageNamespace, treeID TreeID) (ValueIterator, error)

	// Diff receives two trees and a 3rd merge base tree used to resolve the change type
	// it tracks changes from left to right, returning an iterator of Diff entries
	Diff(ctx context.Context, ns StorageNamespace, left, right, base TreeID) (DiffIterator, error)

	// Merge receives two trees and a 3rd merge base tree used to resolve the change type
	// it applies that changes from left to right, resulting in a new tree that
	// is expected to be immediately addressable
	Merge(ctx context.Context, ns StorageNamespace, left, right, base TreeID) (TreeID, error)

	// Apply is the act of taking an existing tree (snapshot) and applying a set of changes to it.
	// A change is either an entity to write/overwrite, or a tombstone to mark a deletion
	// it returns a new treeID that is expected to be immediately addressable
	Apply(ctx context.Context, ns StorageNamespace, treeID TreeID, iterator ValueIterator) (TreeID, error)
}

// StagingManager manages entries in a staging area, denoted by a staging token
// provides basic CRUD abilities, with deletes being written as tombstones (null entry)
type StagingManager interface {
	// GetEntry returns the valure for the provided path (or nil entry to represent a tombstone)
	// Returns ErrNotFound if no value found on key
	Get(ctx context.Context, st StagingToken, key Key) (*Value, error)

	// Set writes a value (or a nil value to represent a tombstone)
	Set(ctx context.Context, st StagingToken, key Key, value *Value) error

	// Delete deletes a value by key
	Delete(ctx context.Context, st StagingToken, key Key) error

	// List returns a ValueIterator for the given staging token
	List(ctx context.Context, st StagingToken) (ValueIterator, error)

	// Drop clears the given staging area
	Drop(ctx context.Context, st StagingToken) error
}

var (
	reValidBranchID     = regexp.MustCompile(`^\w[-\w]*$`)
	reValidRepositoryID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
)

// Graveler errors
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
	u, err := url.Parse(ns)
	if err != nil || u.Scheme == "" {
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

func NewKey(id string) (Key, error) {
	return Key(id), nil
}

func (id Key) String() string {
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
