package graveler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/logging"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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

type DiffSummary struct {
	Count map[DiffType]int
}

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

type MetaRangeData struct {
	// URI of metarange file.
	Address string
}

type RangeData struct {
	// URI of range file.
	Address string
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

type CommitParents []CommitID

// BranchID is an identifier for a branch
type BranchID string

// CommitID is a content addressable hash representing a Commit object
type CommitID string

// MetaRangeID represents a snapshot of the MetaRange, referenced by a commit
type MetaRangeID string

// RangeID represents a part of a MetaRange, useful only for plumbing.
type RangeID string

// StagingToken represents a namespace for writes to apply as uncommitted
type StagingToken string

// Metadata key/value strings to holds metadata information on value and commit
type Metadata map[string]string

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

func (v *ValueRecord) IsTombstone() bool {
	return v.Value == nil
}

func (cp CommitParents) Identity() []byte {
	commits := make([]string, len(cp))
	for i, v := range cp {
		commits[i] = string(v)
	}
	buf := ident.NewAddressWriter()
	buf.MarshalStringSlice(commits)
	return buf.Identity()
}

func (cp CommitParents) Contains(commitID CommitID) bool {
	for _, c := range cp {
		if c == commitID {
			return true
		}
	}
	return false
}

func (cp CommitParents) AsStringSlice() []string {
	stringSlice := make([]string, len(cp))
	for i, p := range cp {
		stringSlice[i] = string(p)
	}
	return stringSlice
}

// FirstCommitMsg is the message of the first (zero) commit of a lakeFS repository
const FirstCommitMsg = "Repository created"

// Commit represents commit metadata (author, time, MetaRangeID)
type Commit struct {
	Committer    string        `db:"committer"`
	Message      string        `db:"message"`
	MetaRangeID  MetaRangeID   `db:"meta_range_id"`
	CreationDate time.Time     `db:"creation_date"`
	Parents      CommitParents `db:"parents"`
	Metadata     Metadata      `db:"metadata"`
}

func (c Commit) Identity() []byte {
	b := ident.NewAddressWriter()
	b.MarshalString("commit:v1")
	b.MarshalString(c.Committer)
	b.MarshalString(c.Message)
	b.MarshalString(string(c.MetaRangeID))
	b.MarshalInt64(c.CreationDate.Unix())
	b.MarshalStringMap(c.Metadata)
	b.MarshalIdentifiable(c.Parents)
	return b.Identity()
}

// CommitRecord holds CommitID with the associated Commit data
type CommitRecord struct {
	CommitID CommitID `db:"id"`
	*Commit
}

// Branch is a pointer to a commit
type Branch struct {
	CommitID     CommitID
	StagingToken StagingToken
}

// BranchRecord holds BranchID with the associated Branch data
type BranchRecord struct {
	BranchID BranchID
	*Branch
}

// TagRecord holds TagID with the associated Tag data
type TagRecord struct {
	TagID    TagID
	CommitID CommitID
}

// Diff represents a change in value based on key
type Diff struct {
	Type         DiffType
	Key          Key
	Value        *Value
	LeftIdentity []byte // the Identity of the value on the left side of the diff
}

func (d *Diff) Copy() *Diff {
	return &Diff{
		Type:         d.Type,
		Key:          d.Key.Copy(),
		Value:        d.Value,
		LeftIdentity: append([]byte(nil), d.LeftIdentity...),
	}
}

type CommitParams struct {
	Committer string
	Message   string
	Metadata  Metadata
}

type KeyValueStore interface {
	// Get returns value from repository / reference by key, nil value is a valid value for tombstone
	// returns error if value does not exist
	Get(ctx context.Context, repositoryID RepositoryID, ref Ref, key Key) (*Value, error)

	// Set stores value on repository / branch by key. nil value is a valid value for tombstone
	Set(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key, value Value) error

	// Delete value from repository / branch branch by key
	Delete(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error

	// List lists values on repository / ref
	List(ctx context.Context, repositoryID RepositoryID, ref Ref) (ValueIterator, error)
}

type VersionController interface {
	// GetRepository returns the Repository metadata object for the given RepositoryID
	GetRepository(ctx context.Context, repositoryID RepositoryID) (*Repository, error)

	// CreateRepository stores a new Repository under RepositoryID with the given Branch as default branch
	CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*Repository, error)

	// CreateBareRepository stores a new Repository under RepositoryID with no initial branch or commit
	CreateBareRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, defaultBranchID BranchID) (*Repository, error)

	// ListRepositories returns iterator to scan repositories
	ListRepositories(ctx context.Context) (RepositoryIterator, error)

	// DeleteRepository deletes the repository
	DeleteRepository(ctx context.Context, repositoryID RepositoryID) error

	// CreateBranch creates branch on repository pointing to ref
	CreateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error)

	// UpdateBranch updates branch on repository pointing to ref
	UpdateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error)

	// GetBranch gets branch information by branch / repository id
	GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*Branch, error)

	// GetTag gets tag's commit id
	GetTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) (*CommitID, error)

	// CreateTag creates tag on a repository pointing to a commit id
	CreateTag(ctx context.Context, repositoryID RepositoryID, tagID TagID, commitID CommitID) error

	// DeleteTag remove tag from a repository
	DeleteTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) error

	// ListTags lists tags on a repository
	ListTags(ctx context.Context, repositoryID RepositoryID) (TagIterator, error)

	// Log returns an iterator starting at commit ID up to repository root
	Log(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (CommitIterator, error)

	// ListBranches lists branches on repositories
	ListBranches(ctx context.Context, repositoryID RepositoryID) (BranchIterator, error)

	// DeleteBranch deletes branch from repository
	DeleteBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error

	// Commit the staged data and returns a commit ID that references that change
	//   ErrNothingToCommit in case there is no data in stage
	Commit(ctx context.Context, repositoryID RepositoryID, branchID BranchID, commitParams CommitParams) (CommitID, error)

	// WriteMetaRange accepts a ValueIterator and writes the entire iterator to a new MetaRange
	// and returns the result ID.
	WriteMetaRange(ctx context.Context, repositoryID RepositoryID, it ValueIterator) (*MetaRangeID, error)

	// AddCommitToBranchHead creates a commit in the branch from the given pre-existing tree.
	// Returns ErrMetaRangeNotFound if the referenced metaRangeID doesn't exist.
	// Returns ErrCommitNotHeadBranch if the branch is no longer referencing to the parentCommit
	AddCommitToBranchHead(ctx context.Context, repositoryID RepositoryID, branchID BranchID, commit Commit) (CommitID, error)

	// AddCommit creates a dangling (no referencing branch) commit in the repo from the pre-existing commit.
	// Returns ErrMetaRangeNotFound if the referenced metaRangeID doesn't exist.
	AddCommit(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error)

	// GetCommit returns the Commit metadata object for the given CommitID
	GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error)

	// Dereference returns the commit ID based on 'ref' reference
	Dereference(ctx context.Context, repositoryID RepositoryID, ref Ref) (CommitID, error)

	// Reset throws all staged data on the repository / branch
	Reset(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error

	// Reset throws all staged data under the specified key on the repository / branch
	ResetKey(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error

	// Reset throws all staged data starting with the given prefix on the repository / branch
	ResetPrefix(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error

	// Revert creates a reverse patch to the commit given as 'ref', and applies it as a new commit on the given branch.
	Revert(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref, parentNumber int, commitParams CommitParams) (CommitID, DiffSummary, error)

	// Merge merges 'source' into 'destination' and returns the commit id for the created merge commit, and a summary of results.
	Merge(ctx context.Context, repositoryID RepositoryID, destination BranchID, source Ref, commitParams CommitParams) (CommitID, DiffSummary, error)

	// DiffUncommitted returns iterator to scan the changes made on the branch
	DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (DiffIterator, error)

	// Diff returns the changes between 'left' and 'right' ref.
	// This is similar to a two-dot (left..right) diff in git.
	Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref) (DiffIterator, error)

	// Compare returns the difference between the commit where 'to' was last synced into 'from', and the most recent commit of `from`.
	// This is similar to a three-dot (from...to) diff in git.
	Compare(ctx context.Context, repositoryID RepositoryID, from, to Ref) (DiffIterator, error)

	// SetHooksHandler set handler for all graveler hooks
	SetHooksHandler(handler HooksHandler)
}

// Plumbing includes commands for fiddling more directly with graveler implementation
// internals.
type Plumbing interface {
	// GetMetarange returns information where metarangeID is stored.
	GetMetaRange(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) (MetaRangeData, error)
	// GetRange returns information where rangeID is stored.
	GetRange(ctx context.Context, repositoryID RepositoryID, rangeID RangeID) (RangeData, error)
}

type Dumper interface {
	// DumpCommits iterates through all commits and dumps them in Graveler format
	DumpCommits(ctx context.Context, repositoryID RepositoryID) (*MetaRangeID, error)

	// DumpBranches iterates through all branches and dumps them in Graveler format
	DumpBranches(ctx context.Context, repositoryID RepositoryID) (*MetaRangeID, error)

	// DumpTags iterates through all tags and dumps them in Graveler format
	DumpTags(ctx context.Context, repositoryID RepositoryID) (*MetaRangeID, error)
}

type Loader interface {
	// LoadCommits iterates through all commits in Graveler format and loads them into repositoryID
	LoadCommits(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) error

	// LoadBranches iterates through all branches in Graveler format and loads them into repositoryID
	LoadBranches(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) error

	// LoadTags iterates through all tags in Graveler format and loads them into repositoryID
	LoadTags(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) error
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
// 'Value()' should only be called after `Next()` returns true.
// In case `Next()` returns false, `Value()` returns nil and `Err()` should be checked.
// nil error means we reached the end of the input.
// `SeekGE()` behaviour is like as starting a new iterator - `Value()` returns nothing until the first `Next()`.

type RepositoryIterator interface {
	Next() bool
	SeekGE(id RepositoryID)
	Value() *RepositoryRecord
	Err() error
	Close()
}

type ValueIterator interface {
	Next() bool
	SeekGE(id Key)
	Value() *ValueRecord
	Err() error
	Close()
}

type DiffIterator interface {
	Next() bool
	SeekGE(id Key)
	Value() *Diff
	Err() error
	Close()
}

type BranchIterator interface {
	Next() bool
	SeekGE(id BranchID)
	Value() *BranchRecord
	Err() error
	Close()
}

type TagIterator interface {
	Next() bool
	SeekGE(id TagID)
	Value() *TagRecord
	Err() error
	Close()
}

type CommitIterator interface {
	Next() bool
	SeekGE(id CommitID)
	Value() *CommitRecord
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
	CreateRepository(ctx context.Context, repositoryID RepositoryID, repository Repository, token StagingToken) error

	// CreateBareRepository stores a new repository under RepositoryID without creating an initial commit and branch
	CreateBareRepository(ctx context.Context, repositoryID RepositoryID, repository Repository) error

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

	// GetTag returns the Tag metadata object for the given TagID
	GetTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) (*CommitID, error)

	// CreateTag create a given tag pointing to a commit
	CreateTag(ctx context.Context, repositoryID RepositoryID, tagID TagID, commitID CommitID) error

	// DeleteTag deletes the tag
	DeleteTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) error

	// ListTags lists tags
	ListTags(ctx context.Context, repositoryID RepositoryID) (TagIterator, error)

	// GetCommit returns the Commit metadata object for the given CommitID.
	GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error)

	// AddCommit stores the Commit object, returning its ID
	AddCommit(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error)

	// FindMergeBase returns the merge-base for the given CommitIDs
	// see: https://git-scm.com/docs/git-merge-base
	// and internally: https://github.com/treeverse/lakeFS/blob/09954804baeb36ada74fa17d8fdc13a38552394e/index/dag/commits.go
	FindMergeBase(ctx context.Context, repositoryID RepositoryID, commitIDs ...CommitID) (*Commit, error)

	// Log returns an iterator starting at commit ID up to repository root
	Log(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (CommitIterator, error)

	// ListCommits returns an iterator over all known commits, ordered by their commit ID
	ListCommits(ctx context.Context, repositoryID RepositoryID) (CommitIterator, error)
}

// CommittedManager reads and applies committed snapshots
// it is responsible for de-duping them, persisting them and providing basic diff, merge and list capabilities
type CommittedManager interface {
	// Get returns the provided key, if exists, from the provided MetaRangeID
	Get(ctx context.Context, ns StorageNamespace, rangeID MetaRangeID, key Key) (*Value, error)

	// Exists returns true if a MetaRange matching ID exists in namespace ns.
	Exists(ctx context.Context, ns StorageNamespace, id MetaRangeID) (bool, error)

	// WriteMetaRange flushes the iterator to a new MetaRange and returns the created ID.
	WriteMetaRange(ctx context.Context, ns StorageNamespace, it ValueIterator, metadata Metadata) (*MetaRangeID, error)

	// List takes a given tree and returns an ValueIterator
	List(ctx context.Context, ns StorageNamespace, rangeID MetaRangeID) (ValueIterator, error)

	// Diff receives two metaRanges and returns a DiffIterator describing all differences between them.
	// This is similar to a two-dot diff in git (left..right)
	Diff(ctx context.Context, ns StorageNamespace, left, right MetaRangeID) (DiffIterator, error)

	// Compare returns the difference between 'source' and 'destination', relative to a merge base 'base'.
	// This is similar to a three-dot diff in git.
	Compare(ctx context.Context, ns StorageNamespace, destination, source, base MetaRangeID) (DiffIterator, error)

	// Merge applies changes from 'source' to 'destination', relative to a merge base 'base' and
	// returns the ID of the new metarange and a summary of diffs.  This is similar to a
	// git merge operation. The resulting tree is expected to be immediately addressable.
	Merge(ctx context.Context, ns StorageNamespace, destination, source, base MetaRangeID) (MetaRangeID, DiffSummary, error)

	// Apply is the act of taking an existing metaRange (snapshot) and applying a set of changes to it.
	// A change is either an entity to write/overwrite, or a tombstone to mark a deletion
	// it returns a new MetaRangeID that is expected to be immediately addressable
	Apply(ctx context.Context, ns StorageNamespace, rangeID MetaRangeID, iterator ValueIterator) (MetaRangeID, DiffSummary, error)

	// GetMetarange returns information where metarangeID is stored.
	GetMetaRange(ctx context.Context, ns StorageNamespace, metaRangeID MetaRangeID) (MetaRangeData, error)
	// GetRange returns information where rangeID is stored.
	GetRange(ctx context.Context, ns StorageNamespace, rangeID RangeID) (RangeData, error)
}

// StagingManager manages entries in a staging area, denoted by a staging token
type StagingManager interface {
	// Get returns the value for the provided staging token and key
	// Returns ErrNotFound if no value found on key.
	Get(ctx context.Context, st StagingToken, key Key) (*Value, error)

	// Set writes a (possibly nil) value under the given staging token and key.
	Set(ctx context.Context, st StagingToken, key Key, value *Value) error

	// List returns a ValueIterator for the given staging token
	List(ctx context.Context, st StagingToken) (ValueIterator, error)

	// DropKey clears a value by staging token and key
	DropKey(ctx context.Context, st StagingToken, key Key) error

	// Drop clears the given staging area
	Drop(ctx context.Context, st StagingToken) error

	// DropByPrefix drops all keys starting with the given prefix, from the given staging area
	DropByPrefix(ctx context.Context, st StagingToken, prefix Key) error
}

// BranchLockerFunc
type BranchLockerFunc func() (interface{}, error)

// BranchLocker enforces the branch locking logic
// The logic is as follows:
// - Allow concurrent writers to acquire the lock.
// - A Metadata update waits for all current writers to release the lock, and then gets the lock.
// - While a metadata update has the lock or is waiting for the lock, any other operation fails to acquire the lock.
type BranchLocker interface {
	Writer(ctx context.Context, repositoryID RepositoryID, branchID BranchID, lockedFn BranchLockerFunc) (interface{}, error)
	MetadataUpdater(ctx context.Context, repositoryID RepositoryID, branchID BranchID, lockeFn BranchLockerFunc) (interface{}, error)
}

func (id RepositoryID) String() string {
	return string(id)
}

func (ns StorageNamespace) String() string {
	return string(ns)
}

func (id BranchID) String() string {
	return string(id)
}

func (id BranchID) Ref() Ref {
	return Ref(id)
}

func (id Ref) String() string {
	return string(id)
}

func (id Key) Copy() Key {
	keyCopy := make(Key, len(id))
	copy(keyCopy, id)
	return keyCopy
}

func (id Key) String() string {
	return string(id)
}

func (id CommitID) String() string {
	return string(id)
}

func (id CommitID) Ref() Ref {
	return Ref(id)
}

func (id TagID) String() string {
	return string(id)
}

type Graveler struct {
	CommittedManager CommittedManager
	StagingManager   StagingManager
	RefManager       RefManager
	branchLocker     BranchLocker
	hooks            HooksHandler
	log              logging.Logger
}

func NewGraveler(branchLocker BranchLocker, committedManager CommittedManager, stagingManager StagingManager, refManager RefManager) *Graveler {
	return &Graveler{
		CommittedManager: committedManager,
		StagingManager:   stagingManager,
		RefManager:       refManager,
		branchLocker:     branchLocker,
		hooks:            &HooksNoOp{},
		log:              logging.Default().WithField("service_name", "graveler_graveler"),
	}
}

func (g *Graveler) GetRepository(ctx context.Context, repositoryID RepositoryID) (*Repository, error) {
	return g.RefManager.GetRepository(ctx, repositoryID)
}

func (g *Graveler) CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*Repository, error) {
	repo := Repository{
		StorageNamespace: storageNamespace,
		CreationDate:     time.Now(),
		DefaultBranchID:  branchID,
	}
	stagingToken := generateStagingToken(repositoryID, branchID)
	err := g.RefManager.CreateRepository(ctx, repositoryID, repo, stagingToken)
	if err != nil {
		return nil, err
	}
	return &repo, nil
}

func (g *Graveler) CreateBareRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, defaultBranchID BranchID) (*Repository, error) {
	repo := Repository{
		StorageNamespace: storageNamespace,
		CreationDate:     time.Now(),
		DefaultBranchID:  defaultBranchID,
	}
	err := g.RefManager.CreateBareRepository(ctx, repositoryID, repo)
	if err != nil {
		return nil, err
	}
	return &repo, nil
}

func (g *Graveler) ListRepositories(ctx context.Context) (RepositoryIterator, error) {
	return g.RefManager.ListRepositories(ctx)
}

func (g *Graveler) WriteMetaRange(ctx context.Context, repositoryID RepositoryID, it ValueIterator) (*MetaRangeID, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRange(ctx, repo.StorageNamespace, it, nil)
}

func (g *Graveler) DeleteRepository(ctx context.Context, repositoryID RepositoryID) error {
	return g.RefManager.DeleteRepository(ctx, repositoryID)
}

func (g *Graveler) GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error) {
	return g.RefManager.GetCommit(ctx, repositoryID, commitID)
}

func generateStagingToken(repositoryID RepositoryID, branchID BranchID) StagingToken {
	// TODO(Guys): initial implementation, change this
	uid := uuid.New().String()
	return StagingToken(fmt.Sprintf("%s-%s:%s", repositoryID, branchID, uid))
}

func (g *Graveler) CreateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error) {
	// check if branch exists
	_, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
	if !errors.Is(err, ErrNotFound) {
		if err == nil {
			err = ErrBranchExists
		}
		return nil, err
	}

	reference, err := g.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	if reference.CommitID() == "" {
		return nil, ErrCreateBranchNoCommit
	}
	newBranch := Branch{
		CommitID:     reference.CommitID(),
		StagingToken: generateStagingToken(repositoryID, branchID),
	}
	err = g.RefManager.SetBranch(ctx, repositoryID, branchID, newBranch)
	if err != nil {
		return nil, err
	}
	return &newBranch, nil
}

func (g *Graveler) UpdateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error) {
	res, err := g.branchLocker.MetadataUpdater(ctx, repositoryID, branchID, func() (interface{}, error) {
		return g.updateBranchNoLock(ctx, repositoryID, branchID, ref)
	})
	if err != nil {
		return nil, err
	}
	return res.(*Branch), nil
}

func (g *Graveler) updateBranchNoLock(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error) {
	reference, err := g.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}

	curBranch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return nil, err
	}
	// validate no conflict
	// TODO(Guys) return error only on conflicts, currently returns error for any changes on staging
	iter, err := g.StagingManager.List(ctx, curBranch.StagingToken)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	if iter.Next() {
		return nil, ErrConflictFound
	}

	newBranch := Branch{
		CommitID:     reference.CommitID(),
		StagingToken: curBranch.StagingToken,
	}
	err = g.RefManager.SetBranch(ctx, repositoryID, branchID, newBranch)
	if err != nil {
		return nil, err
	}
	return &newBranch, nil
}

func (g *Graveler) GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*Branch, error) {
	return g.RefManager.GetBranch(ctx, repositoryID, branchID)
}

func (g *Graveler) GetTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) (*CommitID, error) {
	return g.RefManager.GetTag(ctx, repositoryID, tagID)
}

func (g *Graveler) CreateTag(ctx context.Context, repositoryID RepositoryID, tagID TagID, commitID CommitID) error {
	return g.RefManager.CreateTag(ctx, repositoryID, tagID, commitID)
}

func (g *Graveler) DeleteTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) error {
	return g.RefManager.DeleteTag(ctx, repositoryID, tagID)
}

func (g *Graveler) ListTags(ctx context.Context, repositoryID RepositoryID) (TagIterator, error) {
	return g.RefManager.ListTags(ctx, repositoryID)
}

func (g *Graveler) Dereference(ctx context.Context, repositoryID RepositoryID, ref Ref) (CommitID, error) {
	reference, err := g.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return "", err
	}
	return reference.CommitID(), nil
}

func (g *Graveler) Log(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (CommitIterator, error) {
	return g.RefManager.Log(ctx, repositoryID, commitID)
}

func (g *Graveler) ListBranches(ctx context.Context, repositoryID RepositoryID) (BranchIterator, error) {
	return g.RefManager.ListBranches(ctx, repositoryID)
}

func (g *Graveler) DeleteBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error {
	_, err := g.branchLocker.MetadataUpdater(ctx, repositoryID, branchID, func() (interface{}, error) {
		branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return nil, err
		}
		err = g.StagingManager.Drop(ctx, branch.StagingToken)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		return nil, g.RefManager.DeleteBranch(ctx, repositoryID, branchID)
	})
	return err
}

func (g *Graveler) Get(ctx context.Context, repositoryID RepositoryID, ref Ref, key Key) (*Value, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	reference, err := g.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	if reference.Type() == ReferenceTypeBranch {
		// try to get from staging, if not found proceed to committed
		branch := reference.Branch()
		value, err := g.StagingManager.Get(ctx, branch.StagingToken, key)
		if !errors.Is(err, ErrNotFound) {
			if err != nil {
				return nil, err
			}
			if value == nil {
				// tombstone
				return nil, ErrNotFound
			}
			return value, nil
		}
	}
	commitID := reference.CommitID()
	commit, err := g.RefManager.GetCommit(ctx, repositoryID, commitID)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Get(ctx, repo.StorageNamespace, commit.MetaRangeID, key)
}

func (g *Graveler) Set(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key, value Value) error {
	_, err := g.branchLocker.Writer(ctx, repositoryID, branchID, func() (interface{}, error) {
		branch, err := g.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return nil, err
		}
		err = g.StagingManager.Set(ctx, branch.StagingToken, key, &value)
		return nil, err
	})
	return err
}

// checkStaged returns true if key is staged on manager at token.  It treats staging manager
// errors by returning "not a tombstone", and is unsafe to use if that matters!
func isStagedTombstone(ctx context.Context, manager StagingManager, token StagingToken, key Key) bool {
	e, err := manager.Get(ctx, token, key)
	if err != nil {
		return false
	}
	return e == nil
}

func (g *Graveler) Delete(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	_, err := g.branchLocker.Writer(ctx, repositoryID, branchID, func() (interface{}, error) {
		repo, err := g.RefManager.GetRepository(ctx, repositoryID)
		if err != nil {
			return nil, err
		}
		branch, err := g.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return nil, err
		}

		// mark err as not found and lookup the branch's commit
		err = ErrNotFound
		if branch.CommitID != "" {
			var commit *Commit
			commit, err = g.RefManager.GetCommit(ctx, repositoryID, branch.CommitID)
			if err != nil {
				return nil, err
			}
			// check key in committed - do we need tombstone?
			_, err = g.CommittedManager.Get(ctx, repo.StorageNamespace, commit.MetaRangeID, key)
		}

		if errors.Is(err, ErrNotFound) {
			// no need for tombstone - drop key from stage
			return nil, g.StagingManager.DropKey(ctx, branch.StagingToken, key)
		}
		if err != nil {
			return nil, err
		}

		// key is in committed, stage its tombstone -- regardless of whether or not it
		// is also in staging.  But... if it already has a tombstone staged, return
		// ErrNotFound.

		// Safe to ignore errors when checking staging (if all delete actions worked):
		// we only give a possible incorrect error message if a tombstone was already
		// staged.
		if isStagedTombstone(ctx, g.StagingManager, branch.StagingToken, key) {
			return nil, ErrNotFound
		}

		return nil, g.StagingManager.Set(ctx, branch.StagingToken, key, nil)
	})
	return err
}

func (g *Graveler) List(ctx context.Context, repositoryID RepositoryID, ref Ref) (ValueIterator, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	reference, err := g.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	commitID := reference.CommitID()
	var metaRangeID MetaRangeID
	if commitID != "" {
		commit, err := g.RefManager.GetCommit(ctx, repositoryID, commitID)
		if err != nil {
			return nil, err
		}
		metaRangeID = commit.MetaRangeID
	}

	listing, err := g.CommittedManager.List(ctx, repo.StorageNamespace, metaRangeID)
	if err != nil {
		return nil, err
	}
	if reference.Type() == ReferenceTypeBranch {
		stagingList, err := g.StagingManager.List(ctx, reference.Branch().StagingToken)
		if err != nil {
			return nil, err
		}
		listing = NewCombinedIterator(stagingList, listing)
	}
	return listing, nil
}

func (g *Graveler) Commit(ctx context.Context, repositoryID RepositoryID, branchID BranchID, params CommitParams) (CommitID, error) {
	res, err := g.branchLocker.MetadataUpdater(ctx, repositoryID, branchID, func() (interface{}, error) {
		repo, err := g.RefManager.GetRepository(ctx, repositoryID)
		if err != nil {
			return "", fmt.Errorf("get repository: %w", err)
		}
		branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return "", fmt.Errorf("get branch: %w", err)
		}

		// fill commit information - use for pre-commit and after adding the commit information used by commit
		commit := Commit{
			Committer:    params.Committer,
			Message:      params.Message,
			CreationDate: time.Now(),
			Metadata:     params.Metadata,
		}
		if branch.CommitID != "" {
			commit.Parents = CommitParents{branch.CommitID}
		}

		eventID := uuid.New()
		err = g.hooks.PreCommitHook(ctx, eventID, RepositoryRecord{RepositoryID: repositoryID, Repository: repo}, branchID, commit)
		if err != nil {
			return "", newHookError("pre-commit", err)
		}

		var branchMetaRangeID MetaRangeID
		if branch.CommitID != "" {
			commit, err := g.RefManager.GetCommit(ctx, repositoryID, branch.CommitID)
			if err != nil {
				return "", fmt.Errorf("get commit: %w", err)
			}
			branchMetaRangeID = commit.MetaRangeID
		}
		changes, err := g.StagingManager.List(ctx, branch.StagingToken)
		if err != nil {
			return "", fmt.Errorf("staging list: %w", err)
		}
		defer changes.Close()

		commit.MetaRangeID, _, err = g.CommittedManager.Apply(ctx, repo.StorageNamespace, branchMetaRangeID, changes)
		if err != nil {
			return "", fmt.Errorf("commit: %w", err)
		}

		// add commit
		newCommit, err := g.RefManager.AddCommit(ctx, repositoryID, commit)
		if err != nil {
			return "", fmt.Errorf("add commit: %w", err)
		}
		err = g.RefManager.SetBranch(ctx, repositoryID, branchID, Branch{
			CommitID:     newCommit,
			StagingToken: newStagingToken(repositoryID, branchID),
		})
		if err != nil {
			return "", fmt.Errorf("set branch commit %s: %w", newCommit, err)
		}
		err = g.StagingManager.Drop(ctx, branch.StagingToken)
		if err != nil {
			g.log.WithContext(ctx).WithFields(logging.Fields{
				"repository_id": repositoryID,
				"branch_id":     branchID,
				"commit_id":     branch.CommitID,
				"message":       params.Message,
				"staging_token": branch.StagingToken,
			}).Error("Failed to drop staging data")
		}
		return newCommit, nil
	})
	if err != nil {
		return "", err
	}
	return res.(CommitID), nil
}

func newStagingToken(repositoryID RepositoryID, branchID BranchID) StagingToken {
	v := strings.Join([]string{repositoryID.String(), branchID.String(), uuid.New().String()}, "-")
	return StagingToken(v)
}

func (g *Graveler) validateCommitParent(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error) {
	if len(commit.Parents) > 1 {
		return "", ErrMultipleParents
	}
	if len(commit.Parents) == 0 {
		return "", nil
	}

	parentCommitID := commit.Parents[0]
	_, err := g.RefManager.GetCommit(ctx, repositoryID, parentCommitID)
	if err != nil {
		return "", fmt.Errorf("get parent commit %s: %w", parentCommitID, err)
	}
	return parentCommitID, nil
}

func (g *Graveler) isCommitExist(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (bool, error) {
	_, err := g.RefManager.GetCommit(ctx, repositoryID, commitID)
	if err == nil {
		// commit already exists
		return true, nil
	}
	if !errors.Is(err, ErrCommitNotFound) {
		return false, fmt.Errorf("getting commit %s: %w", commitID, err)
	}
	return false, nil
}

func (g *Graveler) AddCommitToBranchHead(ctx context.Context, repositoryID RepositoryID, branchID BranchID, commit Commit) (CommitID, error) {
	res, err := g.branchLocker.MetadataUpdater(ctx, repositoryID, branchID, func() (interface{}, error) {
		// parentCommitID should always match the HEAD of the branch.
		// Empty parentCommitID matches first commit of the branch.
		parentCommitID, err := g.validateCommitParent(ctx, repositoryID, commit)
		if err != nil {
			return nil, err
		}

		branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return nil, err
		}
		if branch.CommitID != parentCommitID {
			return nil, ErrCommitNotHeadBranch
		}

		// check if commit already exists.
		commitID := CommitID(ident.NewHexAddressProvider().ContentAddress(commit))
		if exists, err := g.isCommitExist(ctx, repositoryID, commitID); err != nil {
			return nil, err
		} else if exists {
			return commitID, nil
		}

		commitID, err = g.addCommitNoLock(ctx, repositoryID, commit)
		if err != nil {
			return nil, fmt.Errorf("adding commit: %w", err)
		}
		_, err = g.updateBranchNoLock(ctx, repositoryID, branchID, Ref(commitID))
		if err != nil {
			return nil, err
		}
		return commitID, nil
	})
	if err != nil {
		return "", err
	}
	return res.(CommitID), nil
}

func (g *Graveler) AddCommit(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error) {
	// at least a single parent must exists
	if len(commit.Parents) == 0 {
		return "", ErrAddCommitNoParent
	}
	_, err := g.validateCommitParent(ctx, repositoryID, commit)
	if err != nil {
		return "", err
	}

	// check if commit already exists.
	commitID := CommitID(ident.NewHexAddressProvider().ContentAddress(commit))
	if exists, err := g.isCommitExist(ctx, repositoryID, commitID); err != nil {
		return "", err
	} else if exists {
		return commitID, nil
	}

	commitID, err = g.addCommitNoLock(ctx, repositoryID, commit)
	if err != nil {
		return "", fmt.Errorf("adding commit: %w", err)
	}

	return commitID, nil
}

// addCommitNoLock lower API used to add commit into a repository. It will verify that the commit meta-range is accessible but will not lock any metadata update.
func (g *Graveler) addCommitNoLock(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return "", fmt.Errorf("get repository %s: %w", repositoryID, err)
	}

	// verify access to meta range
	ok, err := g.CommittedManager.Exists(ctx, repo.StorageNamespace, commit.MetaRangeID)
	if err != nil {
		return "", fmt.Errorf("checking for meta range %s: %w", commit.MetaRangeID, err)
	}
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrMetaRangeNotFound, commit.MetaRangeID)
	}

	// add commit
	commitID, err := g.RefManager.AddCommit(ctx, repositoryID, commit)
	if err != nil {
		return "", fmt.Errorf("add commit: %w", err)
	}
	return commitID, nil
}

func (g *Graveler) stagingEmpty(ctx context.Context, branch *Branch) (bool, error) {
	stIt, err := g.StagingManager.List(ctx, branch.StagingToken)
	if err != nil {
		return false, fmt.Errorf("staging list (token %s): %w", branch.StagingToken, err)
	}

	defer stIt.Close()

	if stIt.Next() {
		return false, nil
	}

	return true, nil
}

func (g *Graveler) Reset(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error {
	_, err := g.branchLocker.Writer(ctx, repositoryID, branchID, func() (interface{}, error) {
		branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return nil, err
		}
		return nil, g.StagingManager.Drop(ctx, branch.StagingToken)
	})
	return err
}

func (g *Graveler) ResetKey(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	_, err := g.branchLocker.Writer(ctx, repositoryID, branchID, func() (interface{}, error) {
		branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return nil, err
		}
		return nil, g.StagingManager.DropKey(ctx, branch.StagingToken, key)
	})
	return err
}

func (g *Graveler) ResetPrefix(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	_, err := g.branchLocker.Writer(ctx, repositoryID, branchID, func() (interface{}, error) {
		branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return nil, err
		}
		return nil, g.StagingManager.DropByPrefix(ctx, branch.StagingToken, key)
	})
	return err
}

type CommitIDAndSummary struct {
	ID      CommitID
	Summary DiffSummary
}

// Revert creates a reverse patch to the commit given as 'ref', and applies it as a new commit on the given branch.
// This is implemented by merging the parent of 'ref' into the branch, with 'ref' as the merge base.
// Example: consider the following tree: C1 -> C2 -> C3, with the branch pointing at C3.
// To revert C2, we merge C1 into the branch, with C2 as the merge base.
// That is, try to apply the diff from C2 to C1 on the tip of the branch.
// If the commit is a merge commit, 'parentNumber' is the parent number (1-based) relative to which the revert is done.
func (g *Graveler) Revert(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref, parentNumber int, commitParams CommitParams) (CommitID, DiffSummary, error) {
	commitRecord, err := g.getCommitRecordFromRef(ctx, repositoryID, ref)
	if err != nil {
		return "", DiffSummary{}, fmt.Errorf("get commit from ref %s: %w", ref, err)
	}
	if len(commitRecord.Parents) > 1 && parentNumber <= 0 {
		// if commit has more than one parent, must explicitly specify parent number
		return "", DiffSummary{}, ErrRevertMergeNoParent
	}
	if parentNumber > 0 {
		// validate parent is in range:
		if parentNumber > len(commitRecord.Parents) { // parent number is 1-based
			return "", DiffSummary{}, fmt.Errorf("%w: parent %d", ErrRevertParentOutOfRange, parentNumber)
		}
		parentNumber--
	}
	res, err := g.branchLocker.MetadataUpdater(ctx, repositoryID, branchID, func() (interface{}, error) {
		repo, err := g.RefManager.GetRepository(ctx, repositoryID)
		if err != nil {
			return nil, fmt.Errorf("get repo %s: %w", repositoryID, err)
		}
		branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return "", fmt.Errorf("get branch %s: %w", branchID, err)
		}
		if empty, err := g.stagingEmpty(ctx, branch); err != nil {
			return "", err
		} else if !empty {
			return "", ErrDirtyBranch
		}
		var parentMetaRangeID MetaRangeID
		if len(commitRecord.Parents) > 0 {
			parentCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, commitRecord.Parents[parentNumber].Ref())
			if err != nil {
				return "", fmt.Errorf("get commit from ref %s: %w", commitRecord.Parents[parentNumber], err)
			}
			parentMetaRangeID = parentCommit.MetaRangeID
		}
		branchCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, branch.CommitID.Ref())
		if err != nil {
			return "", fmt.Errorf("get commit from ref %s: %w", branch.CommitID, err)
		}
		// merge from the parent to the top of the branch, with the given ref as the merge base:
		metaRangeID, summary, err := g.CommittedManager.Merge(ctx, repo.StorageNamespace, branchCommit.MetaRangeID, parentMetaRangeID, commitRecord.MetaRangeID)
		if err != nil {
			if !errors.Is(err, ErrUserVisible) {
				err = fmt.Errorf("merge: %w", err)
			}
			return "", err
		}
		commit := Commit{
			Committer:    commitParams.Committer,
			Message:      commitParams.Message,
			MetaRangeID:  metaRangeID,
			CreationDate: time.Now(),
			Parents:      []CommitID{branch.CommitID},
			Metadata:     commitParams.Metadata,
		}
		commitID, err := g.RefManager.AddCommit(ctx, repositoryID, commit)
		if err != nil {
			return "", fmt.Errorf("add commit: %w", err)
		}
		err = g.RefManager.SetBranch(ctx, repositoryID, branchID, Branch{
			CommitID:     commitID,
			StagingToken: branch.StagingToken,
		})
		if err != nil {
			return "", fmt.Errorf("set branch: %w", err)
		}
		return &CommitIDAndSummary{commitID, summary}, nil
	})
	if err != nil {
		return "", DiffSummary{}, err
	}
	c := res.(*CommitIDAndSummary)
	return c.ID, c.Summary, nil
}

func (g *Graveler) Merge(ctx context.Context, repositoryID RepositoryID, destination BranchID, source Ref, commitParams CommitParams) (CommitID, DiffSummary, error) {
	eventID := uuid.New()
	res, err := g.branchLocker.MetadataUpdater(ctx, repositoryID, destination, func() (interface{}, error) {
		repo, err := g.RefManager.GetRepository(ctx, repositoryID)
		if err != nil {
			return "", err
		}
		branch, err := g.GetBranch(ctx, repositoryID, destination)
		if err != nil {
			return "", fmt.Errorf("get branch: %w", err)
		}
		empty, err := g.stagingEmpty(ctx, branch)
		if err != nil {
			return "", fmt.Errorf("check if staging empty: %w", err)
		}
		if !empty {
			return "", ErrDirtyBranch
		}
		fromCommit, toCommit, baseCommit, err := g.getCommitsForMerge(ctx, repositoryID, source, Ref(destination))
		if err != nil {
			return "", err
		}
		metaRangeID, summary, err := g.CommittedManager.Merge(ctx, repo.StorageNamespace, toCommit.MetaRangeID, fromCommit.MetaRangeID, baseCommit.MetaRangeID)
		if err != nil {
			if !errors.Is(err, ErrUserVisible) {
				err = fmt.Errorf("merge in CommitManager: %w", err)
			}
			return "", err
		}
		commit := Commit{
			Committer:    commitParams.Committer,
			Message:      commitParams.Message,
			CreationDate: time.Now(),
			MetaRangeID:  metaRangeID,
			Parents:      []CommitID{fromCommit.CommitID, toCommit.CommitID},
			Metadata:     commitParams.Metadata,
		}
		err = g.hooks.PreMergeHook(ctx, eventID, RepositoryRecord{RepositoryID: repositoryID, Repository: repo}, destination, source, commit)
		if err != nil {
			return "", newHookError("pre-merge", err)
		}
		commitID, err := g.RefManager.AddCommit(ctx, repositoryID, commit)
		if err != nil {
			return "", fmt.Errorf("add commit: %w", err)
		}
		branch.CommitID = commitID
		err = g.RefManager.SetBranch(ctx, repositoryID, destination, *branch)
		if err != nil {
			return "", fmt.Errorf("update branch %s: %w", destination, err)
		}
		return &CommitIDAndSummary{commitID, summary}, nil
	})
	if err != nil {
		return "", DiffSummary{}, err
	}
	c := res.(*CommitIDAndSummary)
	return c.ID, c.Summary, nil
}

func (g *Graveler) DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (DiffIterator, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return nil, err
	}
	var metaRangeID MetaRangeID
	if branch.CommitID != "" {
		commit, err := g.RefManager.GetCommit(ctx, repositoryID, branch.CommitID)
		if err != nil {
			return nil, err
		}
		metaRangeID = commit.MetaRangeID
	}

	valueIterator, err := g.StagingManager.List(ctx, branch.StagingToken)
	if err != nil {
		return nil, err
	}
	return NewUncommittedDiffIterator(ctx, g.CommittedManager, valueIterator, repo.StorageNamespace, metaRangeID), nil
}

func (g *Graveler) getCommitRecordFromRef(ctx context.Context, repositoryID RepositoryID, ref Ref) (*CommitRecord, error) {
	reference, err := g.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	commit, err := g.RefManager.GetCommit(ctx, repositoryID, reference.CommitID())
	if err != nil {
		return nil, err
	}
	return &CommitRecord{
		CommitID: reference.CommitID(),
		Commit:   commit,
	}, nil
}

func (g *Graveler) Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref) (DiffIterator, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	leftCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, left)
	if err != nil {
		return nil, err
	}
	rightCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, right)
	if err != nil {
		return nil, err
	}

	return g.CommittedManager.Diff(ctx, repo.StorageNamespace, leftCommit.MetaRangeID, rightCommit.MetaRangeID)
}

func (g *Graveler) Compare(ctx context.Context, repositoryID RepositoryID, from, to Ref) (DiffIterator, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	fromCommit, toCommit, baseCommit, err := g.getCommitsForMerge(ctx, repositoryID, from, to)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Compare(ctx, repo.StorageNamespace, toCommit.MetaRangeID, fromCommit.MetaRangeID, baseCommit.MetaRangeID)
}

func (g *Graveler) SetHooksHandler(handler HooksHandler) {
	if handler == nil {
		g.hooks = &HooksNoOp{}
	} else {
		g.hooks = handler
	}
}

func (g *Graveler) getCommitsForMerge(ctx context.Context, repositoryID RepositoryID, from Ref, to Ref) (*CommitRecord, *CommitRecord, *Commit, error) {
	fromCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, from)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get commit by ref %s: %w", from, err)
	}
	toCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, to)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get commit by branch %s: %w", to, err)
	}
	baseCommit, err := g.RefManager.FindMergeBase(ctx, repositoryID, fromCommit.CommitID, toCommit.CommitID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("find merge base: %w", err)
	}
	if baseCommit == nil {
		return nil, nil, nil, ErrNoMergeBase
	}
	return fromCommit, toCommit, baseCommit, nil
}

func (g *Graveler) LoadCommits(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) error {
	repo, err := g.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	iter, err := g.CommittedManager.List(ctx, repo.StorageNamespace, metaRangeID)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		rawValue := iter.Value()
		commit := &CommitData{}
		err := proto.Unmarshal(rawValue.Data, commit)
		if err != nil {
			return err
		}
		parents := make(CommitParents, len(commit.GetParents()))
		for i, p := range commit.GetParents() {
			parents[i] = CommitID(p)
		}
		commitID, err := g.RefManager.AddCommit(ctx, repositoryID, Commit{
			Committer:    commit.GetCommitter(),
			Message:      commit.GetMessage(),
			MetaRangeID:  MetaRangeID(commit.GetMetaRangeId()),
			CreationDate: commit.GetCreationDate().AsTime(),
			Parents:      parents,
			Metadata:     commit.GetMetadata(),
		})
		if err != nil {
			return err
		}
		// integrity check that we get for free!
		if commitID != CommitID(commit.Id) {
			return fmt.Errorf("commit ID does not match for %s: %w", commitID, ErrInvalidCommitID)
		}
	}
	if iter.Err() != nil {
		return iter.Err()
	}
	return nil
}

func (g *Graveler) LoadBranches(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) error {
	repo, err := g.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	iter, err := g.CommittedManager.List(ctx, repo.StorageNamespace, metaRangeID)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		rawValue := iter.Value()
		branch := &BranchData{}
		err := proto.Unmarshal(rawValue.Data, branch)
		if err != nil {
			return err
		}
		branchID := BranchID(branch.Id)
		err = g.RefManager.SetBranch(ctx, repositoryID, branchID, Branch{
			CommitID:     CommitID(branch.CommitId),
			StagingToken: generateStagingToken(repositoryID, branchID),
		})
		if err != nil {
			return err
		}
	}
	if iter.Err() != nil {
		return iter.Err()
	}
	return nil
}

func (g *Graveler) LoadTags(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) error {
	repo, err := g.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	iter, err := g.CommittedManager.List(ctx, repo.StorageNamespace, metaRangeID)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		rawValue := iter.Value()
		tag := &TagData{}
		err := proto.Unmarshal(rawValue.Data, tag)
		if err != nil {
			return err
		}
		tagID := TagID(tag.Id)
		err = g.RefManager.CreateTag(ctx, repositoryID, tagID, CommitID(tag.CommitId))
		if err != nil {
			return err
		}
	}
	if iter.Err() != nil {
		return iter.Err()
	}
	return nil
}

func (g *Graveler) GetMetaRange(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) (MetaRangeData, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return MetaRangeData{}, nil
	}
	return g.CommittedManager.GetMetaRange(ctx, repo.StorageNamespace, metaRangeID)
}

func (g *Graveler) GetRange(ctx context.Context, repositoryID RepositoryID, rangeID RangeID) (RangeData, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return RangeData{}, nil
	}
	return g.CommittedManager.GetRange(ctx, repo.StorageNamespace, rangeID)
}

func (g *Graveler) DumpCommits(ctx context.Context, repositoryID RepositoryID) (*MetaRangeID, error) {
	repo, err := g.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	iter, err := g.RefManager.ListCommits(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	schema, err := serializeSchemaDefinition(&CommitData{})
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRange(ctx, repo.StorageNamespace,
		commitsToValueIterator(iter),
		Metadata{
			EntityTypeKey:             EntityTypeCommit,
			EntitySchemaKey:           EntitySchemaCommit,
			EntitySchemaDefinitionKey: schema,
		},
	)
}

func (g *Graveler) DumpBranches(ctx context.Context, repositoryID RepositoryID) (*MetaRangeID, error) {
	repo, err := g.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	iter, err := g.RefManager.ListBranches(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	schema, err := serializeSchemaDefinition(&BranchData{})
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRange(ctx, repo.StorageNamespace,
		branchesToValueIterator(iter),
		Metadata{
			EntityTypeKey:             EntityTypeBranch,
			EntitySchemaKey:           EntitySchemaBranch,
			EntitySchemaDefinitionKey: schema,
		},
	)
}

func (g *Graveler) DumpTags(ctx context.Context, repositoryID RepositoryID) (*MetaRangeID, error) {
	repo, err := g.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	iter, err := g.RefManager.ListTags(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	schema, err := serializeSchemaDefinition(&TagData{})
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRange(ctx, repo.StorageNamespace,
		tagsToValueIterator(iter),
		Metadata{
			EntityTypeKey:             EntityTypeTag,
			EntitySchemaKey:           EntitySchemaTag,
			EntitySchemaDefinitionKey: schema,
		},
	)
}

func tagsToValueIterator(src TagIterator) ValueIterator {
	return &tagValueIterator{
		src: src,
	}
}

type tagValueIterator struct {
	src   TagIterator
	value *ValueRecord
	err   error
}

func (t *tagValueIterator) Next() bool {
	if t.err != nil {
		return false
	}
	return t.setValue()
}

func (t *tagValueIterator) setValue() bool {
	if !t.src.Next() {
		return false
	}
	tag := t.src.Value()
	data, err := proto.Marshal(&TagData{
		Id:       string(tag.TagID),
		CommitId: string(tag.CommitID),
	})
	if err != nil {
		t.err = err
		return false
	}
	t.value = &ValueRecord{
		Key: Key(tag.TagID),
		Value: &Value{
			Identity: []byte(tag.CommitID),
			Data:     data,
		},
	}
	return true
}

func (t *tagValueIterator) SeekGE(id Key) {
	t.err = nil
	t.value = nil
	t.src.SeekGE(TagID(id))
}

func (t *tagValueIterator) Value() *ValueRecord {
	return t.value
}

func (t *tagValueIterator) Err() error {
	if t.err != nil {
		return t.err
	}
	return t.src.Err()
}

func (t *tagValueIterator) Close() {
	t.src.Close()
}

func branchesToValueIterator(src BranchIterator) ValueIterator {
	return &branchValueIterator{
		src: src,
	}
}

type branchValueIterator struct {
	src   BranchIterator
	value *ValueRecord
	err   error
}

func (b *branchValueIterator) Next() bool {
	if b.err != nil {
		return false
	}
	return b.setValue()
}

func (b *branchValueIterator) setValue() bool {
	if !b.src.Next() {
		return false
	}
	branch := b.src.Value()
	data, err := proto.Marshal(&BranchData{
		Id:       string(branch.BranchID),
		CommitId: string(branch.CommitID),
	})
	if err != nil {
		b.err = err
		return false
	}
	b.value = &ValueRecord{
		Key: Key(branch.BranchID),
		Value: &Value{
			Identity: []byte(branch.CommitID),
			Data:     data,
		},
	}
	return true
}

func (b *branchValueIterator) SeekGE(id Key) {
	b.err = nil
	b.value = nil
	b.src.SeekGE(BranchID(id))
}

func (b *branchValueIterator) Value() *ValueRecord {
	return b.value
}

func (b *branchValueIterator) Err() error {
	if b.err != nil {
		return b.err
	}
	return b.src.Err()
}

func (b *branchValueIterator) Close() {
	b.src.Close()
}

func commitsToValueIterator(src CommitIterator) ValueIterator {
	return &commitValueIterator{
		src: src,
	}
}

type commitValueIterator struct {
	src   CommitIterator
	value *ValueRecord
	err   error
}

func (c *commitValueIterator) Next() bool {
	if c.err != nil {
		return false
	}
	return c.setValue()
}

func (c *commitValueIterator) setValue() bool {
	if !c.src.Next() {
		return false
	}
	commit := c.src.Value()
	data, err := proto.Marshal(&CommitData{
		Id:           string(commit.CommitID),
		Committer:    commit.Committer,
		Message:      commit.Message,
		CreationDate: timestamppb.New(commit.CreationDate),
		MetaRangeId:  string(commit.MetaRangeID),
		Metadata:     commit.Metadata,
		Parents:      commit.Parents.AsStringSlice(),
	})
	if err != nil {
		c.err = err
		return false
	}
	c.value = &ValueRecord{
		Key: Key(commit.CommitID),
		Value: &Value{
			Identity: []byte(commit.CommitID),
			Data:     data,
		},
	}
	return true
}

func (c *commitValueIterator) SeekGE(id Key) {
	c.err = nil
	c.value = nil
	c.src.SeekGE(CommitID(id))
}

func (c *commitValueIterator) Value() *ValueRecord {
	return c.value
}

func (c *commitValueIterator) Err() error {
	if c.err != nil {
		return c.err
	}
	return c.src.Err()
}

func (c *commitValueIterator) Close() {
	c.src.Close()
}

func newHookError(hookEvent string, err error) error {
	err = fmt.Errorf("%s hook: %w", hookEvent, err)
	merr := multierror.Append(ErrAbortedByHook, err)
	merr.ErrorFormat = func(errs []error) string {
		const minErrorLen = 2
		if len(errs) < minErrorLen {
			return multierror.ListFormatFunc(errs)
		}
		var details string
		if len(errs) == minErrorLen {
			details = errs[1].Error()
		} else {
			details = multierror.ListFormatFunc(errs[1:])
		}
		return errs[0].Error() + ": " + details
	}
	return merr
}
