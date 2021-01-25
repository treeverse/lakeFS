package graveler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/logging"
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

type CommitParents []CommitID

// BranchID is an identifier for a branch
type BranchID string

// CommitID is a content addressable hash representing a Commit object
type CommitID string

// MetaRangeID represents a snapshot of the MetaRange, referenced by a commit
type MetaRangeID string

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

func (ps CommitParents) Identity() []byte {
	commits := make([]string, len(ps))
	for i, v := range ps {
		commits[i] = string(v)
	}
	buf := ident.NewAddressWriter()
	buf.MarshalStringSlice(commits)
	return buf.Identity()
}

func (ps CommitParents) Contains(commitID CommitID) bool {
	for _, c := range ps {
		if c == commitID {
			return true
		}
	}
	return false
}

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

// Interfaces
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
	Commit(ctx context.Context, repositoryID RepositoryID, branchID BranchID, committer string, message string, metadata Metadata) (CommitID, error)

	// WriteMetaRange accepts a ValueIterator and writes the entire iterator to a new MetaRange
	// and returns the result ID.
	WriteMetaRange(ctx context.Context, repositoryID RepositoryID, it ValueIterator) (*MetaRangeID, error)

	// CommitExistingMetaRange creates a commit in the branch from the given pre-existing tree.
	// Returns ErrMetaRangeNotFound if the referenced metaRangeID doesn't exist.
	CommitExistingMetaRange(ctx context.Context, repositoryID RepositoryID, parentCommit CommitID, metaRangeID MetaRangeID, committer string, message string, metadata Metadata) (CommitID, error)

	// AddCommitNoLock creates a commit and associates it with the repository without locking metadata for update.
	// Returns ErrTreeNotFound if the referenced treeID doesn't exist.
	AddCommitNoLock(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error)

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
	Revert(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref, committer string, message string, metadata Metadata) (CommitID, error)

	// Merge merge 'from' with 'to' branches under repository returns the new commit id on 'to' branch
	Merge(ctx context.Context, repositoryID RepositoryID, from Ref, to BranchID, committer string, message string, metadata Metadata) (CommitID, error)

	// DiffUncommitted returns iterator to scan the changes made on the branch
	DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (DiffIterator, error)

	// Diff returns the changes between 'left' and 'right' ref.
	// This is similar to a two-dot (left..right) diff in git.
	Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref) (DiffIterator, error)

	// Compare returns the difference between the commit where 'to' was last synced into 'from', and the most recent commit of `from`.
	// This is similar to a three-dot (from...to) diff in git.
	Compare(ctx context.Context, repositoryID RepositoryID, from, to Ref) (DiffIterator, error)
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
}

// CommittedManager reads and applies committed snapshots
// it is responsible for de-duping them, persisting them and providing basic diff, merge and list capabilities
type CommittedManager interface {
	// Get returns the provided key, if exists, from the provided MetaRangeID
	Get(ctx context.Context, ns StorageNamespace, rangeID MetaRangeID, key Key) (*Value, error)

	// Exists returns true if a MetaRange matching ID exists in namespace ns.
	Exists(ctx context.Context, ns StorageNamespace, id MetaRangeID) (bool, error)

	// WriteMetaRange flushes the iterator to a new MetaRange and returns the created ID.
	WriteMetaRange(ctx context.Context, ns StorageNamespace, it ValueIterator) (*MetaRangeID, error)

	// List takes a given tree and returns an ValueIterator
	List(ctx context.Context, ns StorageNamespace, rangeID MetaRangeID) (ValueIterator, error)

	// Diff receives two metaRanges and returns a DiffIterator describing all differences between them.
	// This is similar to a two-dot diff in git (left..right)
	Diff(ctx context.Context, ns StorageNamespace, left, right MetaRangeID) (DiffIterator, error)

	// Compare returns the difference between 'theirs' and 'ours', relative to a merge base 'base'.
	// This is similar to a three-dot diff in git.
	Compare(ctx context.Context, ns StorageNamespace, theirs, ours, base MetaRangeID) (DiffIterator, error)

	// Merge applies that changes from ours to theirs, relative to a merge base 'base'.
	// This is similar to a git merge operation.
	// The resulting tree is expected to be immediately addressable.
	Merge(ctx context.Context, ns StorageNamespace, theirs, ours, base MetaRangeID) (MetaRangeID, error)

	// Apply is the act of taking an existing metaRange (snapshot) and applying a set of changes to it.
	// A change is either an entity to write/overwrite, or a tombstone to mark a deletion
	// it returns a new MetaRangeID that is expected to be immediately addressable
	Apply(ctx context.Context, ns StorageNamespace, rangeID MetaRangeID, iterator ValueIterator) (MetaRangeID, error)
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

type Graveler struct {
	CommittedManager CommittedManager
	StagingManager   StagingManager
	RefManager       RefManager
	branchLocker     BranchLocker
	log              logging.Logger
}

func NewGraveler(branchLocker BranchLocker, committedManager CommittedManager, stagingManager StagingManager, refManager RefManager) *Graveler {
	return &Graveler{
		CommittedManager: committedManager,
		StagingManager:   stagingManager,
		RefManager:       refManager,
		branchLocker:     branchLocker,
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
	branch := Branch{
		StagingToken: generateStagingToken(repositoryID, branchID),
	}
	err := g.RefManager.CreateRepository(ctx, repositoryID, repo, branch)
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
	return g.CommittedManager.WriteMetaRange(ctx, repo.StorageNamespace, it)
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
	})
	if err != nil {
		return nil, err
	}
	return res.(*Branch), nil
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
		commit, err := g.RefManager.GetCommit(ctx, repositoryID, branch.CommitID)
		if err != nil {
			return nil, err
		}

		// check key in committed - do we need tombstone?
		_, err = g.CommittedManager.Get(ctx, repo.StorageNamespace, commit.MetaRangeID, key)
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

func (g *Graveler) Commit(ctx context.Context, repositoryID RepositoryID, branchID BranchID, committer string, message string, metadata Metadata) (CommitID, error) {
	res, err := g.branchLocker.MetadataUpdater(ctx, repositoryID, branchID, func() (interface{}, error) {
		repo, err := g.RefManager.GetRepository(ctx, repositoryID)
		if err != nil {
			return "", fmt.Errorf("get repository: %w", err)
		}
		branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return "", fmt.Errorf("get branch: %w", err)
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
		metaRangeID, err := g.CommittedManager.Apply(ctx, repo.StorageNamespace, branchMetaRangeID, changes)
		if err != nil {
			return "", fmt.Errorf("apply: %w", err)
		}

		// fill and add commit
		commit := Commit{
			Committer:    committer,
			Message:      message,
			MetaRangeID:  metaRangeID,
			CreationDate: time.Now(),
			Metadata:     metadata,
		}
		if branch.CommitID != "" {
			commit.Parents = CommitParents{branch.CommitID}
		}

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
				"message":       message,
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

func (g *Graveler) CommitExistingMetaRange(ctx context.Context, repositoryID RepositoryID, parentCommitID CommitID, metaRangeID MetaRangeID, committer string, message string, metadata Metadata) (CommitID, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return "", fmt.Errorf("get repository %s: %w", repositoryID, err)
	}

	if parentCommitID != "" {
		_, err = g.RefManager.GetCommit(ctx, repositoryID, parentCommitID)
		if err != nil {
			return "", fmt.Errorf("get parent commit %s: %w", parentCommitID, err)
		}
	}

	ok, err := g.CommittedManager.Exists(ctx, repo.StorageNamespace, metaRangeID)
	if err != nil {
		return "", fmt.Errorf("checking for metarange %s: %w", metaRangeID, err)
	}
	if !ok {
		return "", ErrMetaRangeNotFound
	}

	commit := Commit{
		Committer:    committer,
		Message:      message,
		MetaRangeID:  metaRangeID,
		CreationDate: time.Now(),
		Metadata:     metadata,
	}
	if parentCommitID != "" {
		commit.Parents = CommitParents{parentCommitID}
	}

	// check if commit already exists.
	commitID := CommitID(ident.NewHexAddressProvider().ContentAddress(commit))
	_, err = g.RefManager.GetCommit(ctx, repositoryID, commitID)
	if err == nil {
		// commit already exists
		return commitID, nil
	}
	if !errors.Is(err, ErrCommitNotFound) {
		return "", fmt.Errorf("getting commit %s: %w", commitID, err)
	}

	newCommit, err := g.RefManager.AddCommit(ctx, repositoryID, commit)
	if err != nil {
		return "", fmt.Errorf("add commit: %w", err)
	}

	return newCommit, nil
}

// AddCommitNoLock lower API used to add commit into a repository. It will verify that the commit meta-range is accessible but will not lock any metadata update.
func (g *Graveler) AddCommitNoLock(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error) {
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

// Revert creates a reverse patch to the commit given as 'ref', and applies it as a new commit on the given branch.
// This is implemented by merging the parent of 'ref' into the branch, with 'ref' as the merge base.
// Example: consider the following tree: C1 -> C2 -> C3, with the branch pointing at C3.
// To revert C2, we merge C1 into the branch, with C2 as the merge base.
// That is, try to apply the diff from C2 to C1 on the tip of the branch.
func (g *Graveler) Revert(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref, committer string, message string, metadata Metadata) (CommitID, error) {
	commitID, err := g.branchLocker.MetadataUpdater(ctx, repositoryID, branchID, func() (interface{}, error) {
		commitRecord, err := g.getCommitRecordFromRef(ctx, repositoryID, ref)
		if err != nil {
			return "", fmt.Errorf("get commit from ref %s: %w", ref, err)
		}
		if len(commitRecord.Parents) > 1 {
			// TODO(johnnyaug) support reverting merge commits
			return "", ErrRevertMergeCommit
		}
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
			parentCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, commitRecord.Parents[0].Ref())
			if err != nil {
				return "", fmt.Errorf("get commit from ref %s: %w", commitRecord.Parents[0], err)
			}
			parentMetaRangeID = parentCommit.MetaRangeID
		}
		branchCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, branch.CommitID.Ref())
		if err != nil {
			return "", fmt.Errorf("get commit from ref %s: %w", branch.CommitID, err)
		}
		// merge from the parent to the top of the branch, with the given ref as the merge base:
		metaRangeID, err := g.CommittedManager.Merge(ctx, repo.StorageNamespace, branchCommit.MetaRangeID, parentMetaRangeID, commitRecord.MetaRangeID)
		if err != nil {
			return "", fmt.Errorf("merge: %w", err)
		}
		commit := Commit{
			Committer:    committer,
			Message:      message,
			MetaRangeID:  metaRangeID,
			CreationDate: time.Now(),
			Parents:      []CommitID{branch.CommitID},
			Metadata:     metadata,
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
		return commitID, nil
	})
	if err != nil {
		return "", err
	}
	return commitID.(CommitID), nil
}

func (g *Graveler) Merge(ctx context.Context, repositoryID RepositoryID, from Ref, to BranchID, committer string, message string, metadata Metadata) (CommitID, error) {
	res, err := g.branchLocker.MetadataUpdater(ctx, repositoryID, to, func() (interface{}, error) {
		repo, err := g.RefManager.GetRepository(ctx, repositoryID)
		if err != nil {
			return "", err
		}
		branch, err := g.GetBranch(ctx, repositoryID, to)
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
		fromCommit, toCommit, baseCommit, err := g.getCommitsForMerge(ctx, repositoryID, from, Ref(to))
		if err != nil {
			return "", err
		}
		metaRangeID, err := g.CommittedManager.Merge(ctx, repo.StorageNamespace, toCommit.MetaRangeID, fromCommit.MetaRangeID, baseCommit.MetaRangeID)
		if err != nil {
			return "", fmt.Errorf("merge in CommitManager: %w", err)
		}
		commit := Commit{
			Committer:    committer,
			Message:      message,
			MetaRangeID:  metaRangeID,
			CreationDate: time.Now(),
			Parents:      []CommitID{fromCommit.CommitID, toCommit.CommitID},
			Metadata:     metadata,
		}
		commitID, err := g.RefManager.AddCommit(ctx, repositoryID, commit)
		if err != nil {
			return "", fmt.Errorf("add commit: %w", err)
		}
		branch.CommitID = commitID
		err = g.RefManager.SetBranch(ctx, repositoryID, to, *branch)
		if err != nil {
			return "", fmt.Errorf("update branch %s: %w", to, err)
		}
		return commitID, nil
	})
	if err != nil {
		return "", err
	}
	return res.(CommitID), nil
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
