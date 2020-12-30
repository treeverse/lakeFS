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

// CommitParents commit's parents slice
type CommitParents []CommitID

// BranchID is an identifier for a branch
type BranchID string

// CommitID is a content addressable hash representing a Commit object
type CommitID string

// RangeID represents a snapshot of the metaRange, referenced by a commit
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

func (ps CommitParents) Identity() []byte {
	commits := make([]string, len(ps))
	for i, v := range ps {
		commits[i] = string(v)
	}
	buf := ident.NewAddressWriter()
	buf.MarshalStringSlice(commits)
	return buf.Identity()
}

// Commit represents commit metadata (author, time, RangeID)
type Commit struct {
	Committer    string        `db:"committer"`
	Message      string        `db:"message"`
	RangeID      RangeID       `db:"range_id"`
	CreationDate time.Time     `db:"creation_date"`
	Parents      CommitParents `db:"parents"`
	Metadata     Metadata      `db:"metadata"`
}

func (c Commit) Identity() []byte {
	b := ident.NewAddressWriter()
	b.MarshalString("commit:v1")
	b.MarshalString(c.Committer)
	b.MarshalString(c.Message)
	b.MarshalString(string(c.RangeID))
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
	Type        DiffType
	Key         Key
	Value       *Value
	OldIdentity []byte
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

	// Revert commits a change that will revert all the changes make from 'ref' specified
	Revert(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (CommitID, error)

	// Merge merge 'from' with 'to' branches under repository returns the new commit id on 'to' branch
	Merge(ctx context.Context, repositoryID RepositoryID, from Ref, to BranchID, committer string, message string, metadata Metadata) (CommitID, error)

	// DiffUncommitted returns iterator to scan the changes made on the branch
	DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (DiffIterator, error)

	// Diff returns the changes between 'left' and 'right' ref, starting from the 'from' key
	Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref) (DiffIterator, error)
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

// MetaRange abstracts the data structure of the committed data.
type MetaRange interface {
	// ID returns the RangeID
	ID() RangeID
}

// CommittedManager reads and applies committed snapshots
// it is responsible for de-duping them, persisting them and providing basic diff, merge and list capabilities
type CommittedManager interface {
	// Get returns the provided key, if exists, from the provided RangeID
	Get(ctx context.Context, ns StorageNamespace, rangeID RangeID, key Key) (*Value, error)

	// GetMetaRange returns the MetaRange under the namespace with matching ID.
	// If the meta-range is not found, returns ErrNotFound.
	GetMetaRange(ns StorageNamespace, rangeID RangeID) (MetaRange, error)

	// List takes a given meta-range and returns an ValueIterator
	List(ctx context.Context, ns StorageNamespace, rangeID RangeID) (ValueIterator, error)

	// Diff receives two metaRanges and returns a DiffIterator describing all differences between them.
	Diff(ctx context.Context, ns StorageNamespace, left, right RangeID) (DiffIterator, error)

	// Merge receives two metaRanges and a 3rd merge base metaRange used to resolve the change type
	// it applies that changes from left to right, resulting in a new metaRange that
	// is expected to be immediately addressable
	Merge(ctx context.Context, ns StorageNamespace, left, right, base RangeID, committer string, message string, metadata Metadata) (RangeID, error)

	// Apply is the act of taking an existing metaRange (snapshot) and applying a set of changes to it.
	// A change is either an entity to write/overwrite, or a tombstone to mark a deletion
	// it returns a new rangeID that is expected to be immediately addressable
	Apply(ctx context.Context, ns StorageNamespace, rangeID RangeID, iterator ValueIterator) (RangeID, error)
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

var (
	reValidBranchID     = regexp.MustCompile(`^\w[-\w]*$`)
	reValidRepositoryID = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}$`)
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

type graveler struct {
	CommittedManager CommittedManager
	StagingManager   StagingManager
	RefManager       RefManager
	branchLocker     branchLocker
	log              logging.Logger
}

func NewGraveler(committedManager CommittedManager, stagingManager StagingManager, refManager RefManager) *graveler {
	return &graveler{
		CommittedManager: committedManager,
		StagingManager:   stagingManager,
		RefManager:       refManager,
		log:              logging.Default().WithField("service_name", "graveler_graveler"),
		branchLocker:     NewBranchLocker(),
	}
}

func (g *graveler) GetRepository(ctx context.Context, repositoryID RepositoryID) (*Repository, error) {
	return g.RefManager.GetRepository(ctx, repositoryID)
}

func (g *graveler) CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*Repository, error) {
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

func (g *graveler) ListRepositories(ctx context.Context) (RepositoryIterator, error) {
	return g.RefManager.ListRepositories(ctx)
}

func (g *graveler) DeleteRepository(ctx context.Context, repositoryID RepositoryID) error {
	return g.RefManager.DeleteRepository(ctx, repositoryID)
}

func (g *graveler) GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error) {
	return g.RefManager.GetCommit(ctx, repositoryID, commitID)
}

func generateStagingToken(repositoryID RepositoryID, branchID BranchID) StagingToken {
	// TODO(Guys): initial implementation, change this
	uid := uuid.New().String()
	return StagingToken(fmt.Sprintf("%s-%s:%s", repositoryID, branchID, uid))
}

func (g *graveler) CreateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error) {
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

func (g *graveler) UpdateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error) {
	cancel, err := g.branchLocker.AquireMetadataUpdate(repositoryID, branchID)
	if err != nil {
		return nil, err
	}
	defer cancel()
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

func (g *graveler) GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*Branch, error) {
	return g.RefManager.GetBranch(ctx, repositoryID, branchID)
}

func (g *graveler) GetTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) (*CommitID, error) {
	return g.RefManager.GetTag(ctx, repositoryID, tagID)
}

func (g *graveler) CreateTag(ctx context.Context, repositoryID RepositoryID, tagID TagID, commitID CommitID) error {
	return g.RefManager.CreateTag(ctx, repositoryID, tagID, commitID)
}

func (g *graveler) DeleteTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) error {
	return g.RefManager.DeleteTag(ctx, repositoryID, tagID)
}

func (g *graveler) ListTags(ctx context.Context, repositoryID RepositoryID) (TagIterator, error) {
	return g.RefManager.ListTags(ctx, repositoryID)
}

func (g *graveler) Dereference(ctx context.Context, repositoryID RepositoryID, ref Ref) (CommitID, error) {
	reference, err := g.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return "", err
	}
	return reference.CommitID(), nil
}

func (g *graveler) Log(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (CommitIterator, error) {
	return g.RefManager.Log(ctx, repositoryID, commitID)
}

func (g *graveler) ListBranches(ctx context.Context, repositoryID RepositoryID) (BranchIterator, error) {
	return g.RefManager.ListBranches(ctx, repositoryID)
}

func (g *graveler) DeleteBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error {
	cancel, err := g.branchLocker.AquireMetadataUpdate(repositoryID, branchID)
	if err != nil {
		return err
	}
	defer cancel()
	branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	err = g.StagingManager.Drop(ctx, branch.StagingToken)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return g.RefManager.DeleteBranch(ctx, repositoryID, branchID)
}

func (g *graveler) Get(ctx context.Context, repositoryID RepositoryID, ref Ref, key Key) (*Value, error) {
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
	return g.CommittedManager.Get(ctx, repo.StorageNamespace, commit.RangeID, key)
}

func (g *graveler) Set(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key, value Value) error {
	cancel, err := g.branchLocker.AquireWrite(repositoryID, branchID)
	if err != nil {
		return err
	}
	defer cancel()
	branch, err := g.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	return g.StagingManager.Set(ctx, branch.StagingToken, key, &value)
}

func (g *graveler) Delete(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	cancel, err := g.branchLocker.AquireWrite(repositoryID, branchID)
	if err != nil {
		return err
	}
	defer cancel()
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return err
	}
	branch, err := g.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	commit, err := g.RefManager.GetCommit(ctx, repositoryID, branch.CommitID)
	if err != nil {
		return err
	}

	// check key in committed - do we need tombstone?
	_, err = g.CommittedManager.Get(ctx, repo.StorageNamespace, commit.RangeID, key)
	if errors.Is(err, ErrNotFound) {
		// no need for tombstone - drop key from stage
		return g.StagingManager.DropKey(ctx, branch.StagingToken, key)
	}
	if err != nil {
		return err
	}

	// make sure we have tombstone in staging
	entry, err := g.StagingManager.Get(ctx, branch.StagingToken, key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	// return not found if we already got tombstone
	if err == nil && entry == nil {
		return ErrNotFound
	}
	return g.StagingManager.Set(ctx, branch.StagingToken, key, nil)
}

func (g *graveler) List(ctx context.Context, repositoryID RepositoryID, ref Ref) (ValueIterator, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	reference, err := g.RefManager.RevParse(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	commit, err := g.RefManager.GetCommit(ctx, repositoryID, reference.CommitID())
	if err != nil {
		return nil, err
	}

	listing, err := g.CommittedManager.List(ctx, repo.StorageNamespace, commit.RangeID)
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

func (g *graveler) Commit(ctx context.Context, repositoryID RepositoryID, branchID BranchID, committer string, message string, metadata Metadata) (CommitID, error) {
	cancel, err := g.branchLocker.AquireMetadataUpdate(repositoryID, branchID)
	if err != nil {
		return "", fmt.Errorf("acquire metadata update: %w", err)
	}
	defer cancel()
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return "", fmt.Errorf("get repository: %w", err)
	}
	branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return "", fmt.Errorf("get branch: %w", err)
	}
	commit, err := g.RefManager.GetCommit(ctx, repositoryID, branch.CommitID)
	if err != nil {
		return "", fmt.Errorf("get commit: %w", err)
	}
	changes, err := g.StagingManager.List(ctx, branch.StagingToken)
	if err != nil {
		return "", fmt.Errorf("staging list: %w", err)
	}
	rangeID, err := g.CommittedManager.Apply(ctx, repo.StorageNamespace, commit.RangeID, changes)
	if err != nil {
		return "", fmt.Errorf("apply: %w", err)
	}
	newCommit, err := g.RefManager.AddCommit(ctx, repositoryID, Commit{
		Committer:    committer,
		Message:      message,
		RangeID:      rangeID,
		CreationDate: time.Now(),
		Parents:      CommitParents{branch.CommitID},
		Metadata:     metadata,
	})
	if err != nil {
		return "", fmt.Errorf("add commit: %w", err)
	}
	err = g.StagingManager.Drop(ctx, branch.StagingToken)
	if err != nil {
		g.log.WithContext(ctx).WithFields(logging.Fields{
			"repository_id": repositoryID,
			"branch_id":     branchID,
			"commit_id":     *commit,
			"message":       message,
			"staging_token": branch.StagingToken,
		}).Error("Failed to drop staging data")
	}
	return newCommit, nil
}

func (g *graveler) CommitExistingMetaRange(ctx context.Context, repositoryID RepositoryID, branchID BranchID, rangeID RangeID, committer string, message string, metadata Metadata) (CommitID, error) {
	cancel, err := g.branchLocker.AquireMetadataUpdate(repositoryID, branchID)
	if err != nil {
		return "", fmt.Errorf("acquire metadata update: %w", err)
	}
	defer cancel()
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return "", fmt.Errorf("get repository %s: %w", repositoryID, err)
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

	if _, err := g.CommittedManager.GetMetaRange(repo.StorageNamespace, rangeID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", ErrMetaRangeNotFound
		}
		return "", fmt.Errorf("checking for metarange %s: %w", rangeID, err)
	}

	newCommit, err := g.RefManager.AddCommit(ctx, repositoryID, Commit{
		Committer:    committer,
		Message:      message,
		RangeID:      rangeID,
		CreationDate: time.Now(),
		Parents:      CommitParents{branch.CommitID},
		Metadata:     metadata,
	})
	if err != nil {
		return "", fmt.Errorf("add commit: %w", err)
	}

	return newCommit, nil
}

func (g *graveler) stagingEmpty(ctx context.Context, branch *Branch) (bool, error) {
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

func (g *graveler) Reset(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error {
	cancel, err := g.branchLocker.AquireWrite(repositoryID, branchID)
	if err != nil {
		return err
	}
	defer cancel()
	branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	return g.StagingManager.Drop(ctx, branch.StagingToken)
}

func (g *graveler) ResetKey(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	cancel, err := g.branchLocker.AquireWrite(repositoryID, branchID)
	if err != nil {
		return err
	}
	defer cancel()
	branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	return g.StagingManager.DropKey(ctx, branch.StagingToken, key)
}

func (g *graveler) ResetPrefix(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	cancel, err := g.branchLocker.AquireWrite(repositoryID, branchID)
	if err != nil {
		return err
	}
	defer cancel()
	branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return err
	}
	return g.StagingManager.DropByPrefix(ctx, branch.StagingToken, key)
}

func (g *graveler) Revert(_ context.Context, _ RepositoryID, _ BranchID, _ Ref) (CommitID, error) {
	panic("implement me")
}

func (g *graveler) Merge(ctx context.Context, repositoryID RepositoryID, from Ref, to BranchID, committer string, message string, metadata Metadata) (CommitID, error) {
	cancel, err := g.branchLocker.AquireMetadataUpdate(repositoryID, to)
	if err != nil {
		return "", err
	}
	defer cancel()
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return "", err
	}

	fromCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, from)
	if err != nil {
		return "", err
	}
	toCommit, err := g.getCommitRecordFromRef(ctx, repositoryID, Ref(to))
	if err != nil {
		return "", err
	}
	baseCommit, err := g.RefManager.FindMergeBase(ctx, repositoryID, fromCommit.CommitID, toCommit.CommitID)
	if err != nil {
		return "", err
	}

	rangeID, err := g.CommittedManager.Merge(ctx, repo.StorageNamespace, fromCommit.RangeID, toCommit.RangeID, baseCommit.RangeID, committer, message, metadata)
	if err != nil {
		return "", err
	}
	commit := Commit{
		Committer:    committer,
		Message:      message,
		RangeID:      rangeID,
		CreationDate: time.Time{},
		Parents:      []CommitID{fromCommit.CommitID, toCommit.CommitID},
		Metadata:     metadata,
	}
	return g.RefManager.AddCommit(ctx, repositoryID, commit)
}

func (g *graveler) DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (DiffIterator, error) {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	branch, err := g.RefManager.GetBranch(ctx, repositoryID, branchID)
	if err != nil {
		return nil, err
	}
	commit, err := g.RefManager.GetCommit(ctx, repositoryID, branch.CommitID)
	if err != nil {
		return nil, err
	}
	valueIterator, err := g.StagingManager.List(ctx, branch.StagingToken)
	if err != nil {
		return nil, err
	}
	return NewUncommittedDiffIterator(ctx, g.CommittedManager, valueIterator, repo.StorageNamespace, commit.RangeID), nil
}

func (g *graveler) getCommitRecordFromRef(ctx context.Context, repositoryID RepositoryID, ref Ref) (*CommitRecord, error) {
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

func (g *graveler) Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref) (DiffIterator, error) {
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

	return g.CommittedManager.Diff(ctx, repo.StorageNamespace, leftCommit.RangeID, rightCommit.RangeID)
}
