package graveler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/logging"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:generate mockgen -source=graveler.go -destination=mock/graveler.go -package=mock

const (
	ListingDefaultBatchSize = 1000
	ListingMaxBatchSize     = 100000

	MergeStrategySrcWins  = "source-wins"
	MergeStrategyDestWins = "dest-wins"
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

type RefModType rune

const (
	RefModTypeTilde  RefModType = '~'
	RefModTypeCaret  RefModType = '^'
	RefModTypeAt     RefModType = '@'
	RefModTypeDollar RefModType = '$'
)

type RefModifier struct {
	Type  RefModType
	Value int
}

// RawRef is a parsed Ref that includes 'BaseRef' that holds the branch/tag/hash and a list of
//   ordered modifiers that applied to the reference.
// Example: master~2 will be parsed into {BaseRef:"master", Modifiers:[{Type:RefModTypeTilde, Value:2}]}
type RawRef struct {
	BaseRef   string
	Modifiers []RefModifier
}

type DiffSummary struct {
	Count      map[DiffType]int
	Incomplete bool // true when Diff summary has missing Information (could happen when skipping ranges with same bounds)
}

// ReferenceType represents the type of the reference
type ReferenceType uint8

const (
	ReferenceTypeCommit ReferenceType = iota
	ReferenceTypeTag
	ReferenceTypeBranch
)

// ResolvedBranchModifier indicates if the ref specified one of the committed/staging modifiers, and which.
type ResolvedBranchModifier int

const (
	ResolvedBranchModifierNone ResolvedBranchModifier = iota
	ResolvedBranchModifierCommitted
	ResolvedBranchModifierStaging
)

// ResolvedRef include resolved information of Ref/RawRef:
//   Type: Branch / Tag / Commit
//   BranchID: for type ReferenceTypeBranch will hold the branch ID
//   ResolvedBranchModifier: branch indicator if resolved to a branch latest commit, staging or none was specified.
//   CommitID: the commit ID of the branch head,  tag or specific hash.
//   StagingToken: empty if ResolvedBranchModifier is ResolvedBranchModifierCommmitted.
//
type ResolvedRef struct {
	Type                   ReferenceType
	BranchID               BranchID
	ResolvedBranchModifier ResolvedBranchModifier
	CommitID               CommitID
	StagingToken           StagingToken
}

// MergeStrategy changes from dest or source are automatically overridden in case of a conflict
type MergeStrategy int

const (
	MergeStrategyNone MergeStrategy = iota
	MergeStrategyDest
	MergeStrategySource
)

// MetaRangeAddress is the URI of a metarange file.
type MetaRangeAddress string

// RangeAddress is the URI of a range file.
type RangeAddress string

// RangeInfo contains information on a Range
type RangeInfo struct {
	// ID is the identifier for the written Range.
	// Calculated by a hash function to all keys and values' identity.
	ID RangeID

	// MinKey is the first key in the Range.
	MinKey Key

	// MaxKey is the last key in the Range.
	MaxKey Key

	// Count is the number of records in the Range.
	Count int

	// EstimatedRangeSizeBytes is Approximate size of each Range
	EstimatedRangeSizeBytes uint64
}

// MetaRangeInfo contains information on a MetaRange
type MetaRangeInfo struct {
	// ID is the identifier for the written MetaRange.
	// Calculated by an hash function to all keys and values' identity.
	ID MetaRangeID
}

type WriteCondition struct {
	IfAbsent bool
}

type WriteConditionOption func(condition *WriteCondition)

func IfAbsent(v bool) WriteConditionOption {
	return func(condition *WriteCondition) {
		condition.IfAbsent = v
	}
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

// Metadata key/value strings to hold metadata information on value and commit
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

// CommitVersion used to track changes in Commit schema. Each version is change that a constant describes.
type CommitVersion int

const (
	CommitVersionInitial CommitVersion = iota
	CommitVersionParentSwitch

	CurrentCommitVersion = CommitVersionParentSwitch
)

// Change it if a change was applied to the schema of the Commit struct.

// Commit represents commit metadata (author, time, MetaRangeID)
type Commit struct {
	Version      CommitVersion `db:"version"`
	Committer    string        `db:"committer"`
	Message      string        `db:"message"`
	MetaRangeID  MetaRangeID   `db:"meta_range_id"`
	CreationDate time.Time     `db:"creation_date"`
	Parents      CommitParents `db:"parents"`
	Metadata     Metadata      `db:"metadata"`
	Generation   int           `db:"generation"`
}

func NewCommit() Commit {
	return Commit{
		Version:      CurrentCommitVersion,
		CreationDate: time.Now(),
	}
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
	// Date (Unix Epoch in seconds) is used to override commits creation date
	Date     *int64
	Metadata Metadata
	// SourceMetaRange - If exists, use it directly. Fail if branch has uncommitted changes
	SourceMetaRange *MetaRangeID
}

type KeyValueStore interface {
	// Get returns value from repository / reference by key, nil value is a valid value for tombstone
	// returns error if value does not exist
	Get(ctx context.Context, repositoryID RepositoryID, ref Ref, key Key) (*Value, error)

	// Set stores value on repository / branch by key. nil value is a valid value for tombstone
	Set(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key, value Value, writeConditions ...WriteConditionOption) error

	// Delete value from repository / branch by key
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

	// WriteMetaRangeByIterator accepts a ValueIterator and writes the entire iterator to a new MetaRange
	// and returns the result ID.
	WriteMetaRangeByIterator(ctx context.Context, repositoryID RepositoryID, it ValueIterator) (*MetaRangeID, error)

	// AddCommitToBranchHead creates a commit in the branch from the given pre-existing tree.
	// Returns ErrMetaRangeNotFound if the referenced metaRangeID doesn't exist.
	// Returns ErrCommitNotHeadBranch if the branch is no longer referencing to the parentCommit
	AddCommitToBranchHead(ctx context.Context, repositoryID RepositoryID, branchID BranchID, commit Commit) (CommitID, error)

	// AddCommit creates a dangling (no referencing branch) commit in the repo from the pre-existing commit.
	// Returns ErrMetaRangeNotFound if the referenced metaRangeID doesn't exist.
	AddCommit(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error)

	// GetCommit returns the Commit metadata object for the given CommitID
	GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error)

	// Dereference returns the resolved ref information based on 'ref' reference
	Dereference(ctx context.Context, repositoryID RepositoryID, ref Ref) (*ResolvedRef, error)

	// ParseRef returns parsed 'ref' information as raw reference
	ParseRef(ref Ref) (RawRef, error)

	// ResolveRawRef returns the ResolvedRef matching the given RawRef
	ResolveRawRef(ctx context.Context, repositoryID RepositoryID, rawRef RawRef) (*ResolvedRef, error)

	// Reset throws all staged data on the repository / branch
	Reset(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error

	// ResetKey throws all staged data under the specified key on the repository / branch
	ResetKey(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error

	// ResetPrefix throws all staged data starting with the given prefix on the repository / branch
	ResetPrefix(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error

	// Revert creates a reverse patch to the commit given as 'ref', and applies it as a new commit on the given branch.
	Revert(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref, parentNumber int, commitParams CommitParams) (CommitID, error)

	// Merge merges 'source' into 'destination' and returns the commit id for the created merge commit.
	Merge(ctx context.Context, repositoryID RepositoryID, destination BranchID, source Ref, commitParams CommitParams, strategy string) (CommitID, error)

	// DiffUncommitted returns iterator to scan the changes made on the branch
	DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (DiffIterator, error)

	// Diff returns the changes between 'left' and 'right' ref.
	// This is similar to a two-dot (left..right) diff in git.
	Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref) (DiffIterator, error)

	// Compare returns the difference between the commit where 'left' was last synced into 'right', and the most recent commit of `right`.
	// This is similar to a three-dot (from...to) diff in git.
	Compare(ctx context.Context, repositoryID RepositoryID, left, right Ref) (DiffIterator, error)

	// SetHooksHandler set handler for all graveler hooks
	SetHooksHandler(handler HooksHandler)

	// GetStagingToken returns the token identifying current staging for branchID of
	// repositoryID.
	GetStagingToken(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*StagingToken, error)

	GetGarbageCollectionRules(ctx context.Context, repositoryID RepositoryID) (*GarbageCollectionRules, error)

	SetGarbageCollectionRules(ctx context.Context, repositoryID RepositoryID, rules *GarbageCollectionRules) error

	// SaveGarbageCollectionCommits saves the sets of active and expired commits, according to the branch rules for garbage collection.
	// Returns
	//	- run id which can later be used to retrieve the set of commits.
	//	- location where the expired/active commit information was saved
	//	- location where the information of addresses to be removed should be saved
	// If a previousRunID is specified, commits that were already expired and their ancestors will not be considered as expired/active.
	// Note: Ancestors of previously expired commits may still be considered if they can be reached from a non-expired commit.
	SaveGarbageCollectionCommits(ctx context.Context, repositoryID RepositoryID, previousRunID string) (garbageCollectionRunMetadata *GarbageCollectionRunMetadata, err error)

	// GetBranchProtectionRules return all branch protection rules for the repository
	GetBranchProtectionRules(ctx context.Context, repositoryID RepositoryID) (*BranchProtectionRules, error)

	// DeleteBranchProtectionRule deletes the branch protection rule for the given pattern,
	// or return ErrRuleNotExists if no such rule exists.
	DeleteBranchProtectionRule(ctx context.Context, repositoryID RepositoryID, pattern string) error

	// CreateBranchProtectionRule creates a rule for the given name pattern,
	// or returns ErrRuleAlreadyExists if there is already a rule for the pattern.
	CreateBranchProtectionRule(ctx context.Context, repositoryID RepositoryID, pattern string, blockedActions []BranchProtectionBlockedAction) error
}

// Plumbing includes commands for fiddling more directly with graveler implementation
// internals.
type Plumbing interface {
	// GetMetaRange returns information where metarangeID is stored.
	GetMetaRange(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) (MetaRangeAddress, error)
	// GetRange returns information where rangeID is stored.
	GetRange(ctx context.Context, repositoryID RepositoryID, rangeID RangeID) (RangeAddress, error)
	// WriteRange creates a new Range from the iterator values.
	// Keeps Range closing logic, so might not flush all values to the range.
	WriteRange(ctx context.Context, repositoryID RepositoryID, it ValueIterator) (*RangeInfo, error)
	// WriteMetaRange creates a new MetaRange from the given Ranges.
	WriteMetaRange(ctx context.Context, repositoryID RepositoryID, ranges []*RangeInfo) (*MetaRangeInfo, error)
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

	// ParseRef returns parsed 'ref' information as RawRef
	ParseRef(ref Ref) (RawRef, error)

	// ResolveRawRef returns the ResolvedRef matching the given RawRef
	ResolveRawRef(ctx context.Context, repositoryID RepositoryID, rawRef RawRef) (*ResolvedRef, error)

	// GetBranch returns the Branch metadata object for the given BranchID
	GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*Branch, error)

	// CreateBranch creates a branch with the given id and Branch metadata
	CreateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, branch Branch) error

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

	// GetCommitByPrefix returns the Commit metadata object for the given prefix CommitID.
	// if more than 1 commit starts with the ID prefix returns error
	GetCommitByPrefix(ctx context.Context, repositoryID RepositoryID, prefix CommitID) (*Commit, error)

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

	// WriteMetaRangeByIterator flushes the iterator to a new MetaRange and returns the created ID.
	WriteMetaRangeByIterator(ctx context.Context, ns StorageNamespace, it ValueIterator, metadata Metadata) (*MetaRangeID, error)

	// WriteRange creates a new Range from the iterator values.
	// Keeps Range closing logic, so might not exhaust the iterator.
	WriteRange(ctx context.Context, ns StorageNamespace, it ValueIterator) (*RangeInfo, error)

	// WriteMetaRange creates a new MetaRange from the given Ranges.
	WriteMetaRange(ctx context.Context, ns StorageNamespace, ranges []*RangeInfo) (*MetaRangeInfo, error)

	// List takes a given tree and returns an ValueIterator
	List(ctx context.Context, ns StorageNamespace, rangeID MetaRangeID) (ValueIterator, error)

	// Diff receives two metaRanges and returns a DiffIterator describing all differences between them.
	// This is similar to a two-dot diff in git (left..right)
	Diff(ctx context.Context, ns StorageNamespace, left, right MetaRangeID) (DiffIterator, error)

	// Compare returns the difference between 'source' and 'destination', relative to a merge base 'base'.
	// This is similar to a three-dot diff in git.
	Compare(ctx context.Context, ns StorageNamespace, destination, source, base MetaRangeID) (DiffIterator, error)

	// Merge applies changes from 'source' to 'destination', relative to a merge base 'base' and
	// returns the ID of the new metarange. This is similar to a git merge operation.
	// The resulting tree is expected to be immediately addressable.
	Merge(ctx context.Context, ns StorageNamespace, destination, source, base MetaRangeID, strategy MergeStrategy) (MetaRangeID, error)

	// Commit is the act of taking an existing metaRange (snapshot) and applying a set of changes to it.
	// A change is either an entity to write/overwrite, or a tombstone to mark a deletion
	// it returns a new MetaRangeID that is expected to be immediately addressable
	Commit(ctx context.Context, ns StorageNamespace, baseMetaRangeID MetaRangeID, changes ValueIterator) (MetaRangeID, DiffSummary, error)

	// GetMetaRange returns information where metarangeID is stored.
	GetMetaRange(ctx context.Context, ns StorageNamespace, metaRangeID MetaRangeID) (MetaRangeAddress, error)
	// GetRange returns information where rangeID is stored.
	GetRange(ctx context.Context, ns StorageNamespace, rangeID RangeID) (RangeAddress, error)
}

// StagingManager manages entries in a staging area, denoted by a staging token
type StagingManager interface {
	// Get returns the value for the provided staging token and key
	// Returns ErrNotFound if no value found on key.
	Get(ctx context.Context, st StagingToken, key Key) (*Value, error)

	// Set writes a (possibly nil) value under the given staging token and key.
	Set(ctx context.Context, st StagingToken, key Key, value *Value, overwrite bool) error

	// List returns a ValueIterator for the given staging token
	List(ctx context.Context, st StagingToken, batchSize int) (ValueIterator, error)

	// DropKey clears a value by staging token and key
	DropKey(ctx context.Context, st StagingToken, key Key) error

	// Drop clears the given staging area
	Drop(ctx context.Context, st StagingToken) error

	// DropByPrefix drops all keys starting with the given prefix, from the given staging area
	DropByPrefix(ctx context.Context, st StagingToken, prefix Key) error
}

// BranchLockerFunc callback function when branch is locked for operation (ex: writer or metadata updater)
type BranchLockerFunc func() (interface{}, error)

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

type KVGraveler struct {
	db                       *DBGraveler
	hooks                    HooksHandler
	CommittedManager         CommittedManager
	RefManager               RefManager
	StagingManager           StagingManager
	protectedBranchesManager ProtectedBranchesManager
	log                      logging.Logger
}

func NewKVGraveler(branchLocker BranchLocker, committedManager CommittedManager, stagingManager StagingManager, refManager RefManager, gcManager GarbageCollectionManager, protectedBranchesManager ProtectedBranchesManager) *KVGraveler {
	return &KVGraveler{
		db:                       NewDBGraveler(branchLocker, committedManager, stagingManager, refManager, gcManager, protectedBranchesManager),
		hooks:                    &HooksNoOp{},
		CommittedManager:         committedManager,
		RefManager:               refManager,
		StagingManager:           stagingManager,
		protectedBranchesManager: protectedBranchesManager,
		log:                      logging.Default().WithField("service_name", "graveler_graveler"),
	}
}

func (g *KVGraveler) GetRepository(ctx context.Context, repositoryID RepositoryID) (*Repository, error) {
	return g.RefManager.GetRepository(ctx, repositoryID)
}

func (g *KVGraveler) CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*Repository, error) {
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

func (g *KVGraveler) CreateBareRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, defaultBranchID BranchID) (*Repository, error) {
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

func (g *KVGraveler) ListRepositories(ctx context.Context) (RepositoryIterator, error) {
	return g.RefManager.ListRepositories(ctx)
}

func (g *KVGraveler) WriteRange(ctx context.Context, repositoryID RepositoryID, it ValueIterator) (*RangeInfo, error) {
	return g.db.WriteRange(ctx, repositoryID, it)
}

func (g *KVGraveler) WriteMetaRange(ctx context.Context, repositoryID RepositoryID, ranges []*RangeInfo) (*MetaRangeInfo, error) {
	return g.db.WriteMetaRange(ctx, repositoryID, ranges)
}

func (g *KVGraveler) WriteMetaRangeByIterator(ctx context.Context, repositoryID RepositoryID, it ValueIterator) (*MetaRangeID, error) {
	return g.db.WriteMetaRangeByIterator(ctx, repositoryID, it)
}

func (g *KVGraveler) DeleteRepository(ctx context.Context, repositoryID RepositoryID) error {
	return g.RefManager.DeleteRepository(ctx, repositoryID)
}

func (g *KVGraveler) GetCommit(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (*Commit, error) {
	return g.RefManager.GetCommit(ctx, repositoryID, commitID)
}

func generateStagingToken(repositoryID RepositoryID, branchID BranchID) StagingToken {
	uid := uuid.New().String()
	return StagingToken(fmt.Sprintf("%s-%s:%s", repositoryID, branchID, uid))
}

func (g *KVGraveler) CreateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error) {
	return g.db.CreateBranch(ctx, repositoryID, branchID, ref)
}

func (g *KVGraveler) UpdateBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref) (*Branch, error) {
	return g.db.UpdateBranch(ctx, repositoryID, branchID, ref)
}

func (g *KVGraveler) GetBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*Branch, error) {
	return g.db.GetBranch(ctx, repositoryID, branchID)
}

func (g *KVGraveler) GetTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) (*CommitID, error) {
	return g.RefManager.GetTag(ctx, repositoryID, tagID)
}

func (g *KVGraveler) CreateTag(ctx context.Context, repositoryID RepositoryID, tagID TagID, commitID CommitID) error {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return fmt.Errorf("get repository: %w", err)
	}
	storageNamespace := repo.StorageNamespace

	// Check that Tag doesn't exist before running hook - Non-Atomic operation
	_, err = g.RefManager.GetTag(ctx, repositoryID, tagID)
	if err == nil {
		return ErrTagAlreadyExists
	}
	if !errors.Is(err, ErrTagNotFound) {
		return err
	}

	preRunID := g.hooks.NewRunID()
	err = g.hooks.PreCreateTagHook(ctx, HookRecord{
		RunID:            preRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePreCreateTag,
		RepositoryID:     repositoryID,
		CommitID:         commitID,
		SourceRef:        commitID.Ref(),
		TagID:            tagID,
	})
	if err != nil {
		return &HookAbortError{
			EventType: EventTypePreCreateTag,
			RunID:     preRunID,
			Err:       err,
		}
	}

	err = g.RefManager.CreateTag(ctx, repositoryID, tagID, commitID)
	if err != nil {
		return err
	}

	postRunID := g.hooks.NewRunID()
	g.hooks.PostCreateTagHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostCreateTag,
		RepositoryID:     repositoryID,
		CommitID:         commitID,
		SourceRef:        commitID.Ref(),
		TagID:            tagID,
		PreRunID:         preRunID,
	})

	return nil
}

func (g *KVGraveler) DeleteTag(ctx context.Context, repositoryID RepositoryID, tagID TagID) error {
	repo, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil {
		return fmt.Errorf("get repository: %w", err)
	}
	storageNamespace := repo.StorageNamespace

	// Sanity check that Tag exists before running hook.
	commitID, err := g.RefManager.GetTag(ctx, repositoryID, tagID)
	if err != nil {
		return err
	}

	preRunID := g.hooks.NewRunID()
	err = g.hooks.PreDeleteTagHook(ctx, HookRecord{
		RunID:            preRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePreDeleteTag,
		RepositoryID:     repositoryID,
		SourceRef:        commitID.Ref(),
		CommitID:         *commitID,
		TagID:            tagID,
	})
	if err != nil {
		return &HookAbortError{
			EventType: EventTypePreDeleteTag,
			RunID:     preRunID,
			Err:       err,
		}
	}

	err = g.RefManager.DeleteTag(ctx, repositoryID, tagID)
	if err != nil {
		return err
	}

	postRunID := g.hooks.NewRunID()
	g.hooks.PostDeleteTagHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostDeleteTag,
		RepositoryID:     repositoryID,
		SourceRef:        commitID.Ref(),
		CommitID:         *commitID,
		TagID:            tagID,
		PreRunID:         preRunID,
	})
	return nil
}

func (g *KVGraveler) ListTags(ctx context.Context, repositoryID RepositoryID) (TagIterator, error) {
	return g.RefManager.ListTags(ctx, repositoryID)
}

func (g *KVGraveler) Dereference(ctx context.Context, repositoryID RepositoryID, ref Ref) (*ResolvedRef, error) {
	rawRef, err := g.ParseRef(ref)
	if err != nil {
		return nil, err
	}
	return g.ResolveRawRef(ctx, repositoryID, rawRef)
}

func (g *KVGraveler) ParseRef(ref Ref) (RawRef, error) {
	return g.db.ParseRef(ref)
}

func (g *KVGraveler) ResolveRawRef(ctx context.Context, repositoryID RepositoryID, rawRef RawRef) (*ResolvedRef, error) {
	return g.db.ResolveRawRef(ctx, repositoryID, rawRef)
}

func (g *KVGraveler) Log(ctx context.Context, repositoryID RepositoryID, commitID CommitID) (CommitIterator, error) {
	return g.RefManager.Log(ctx, repositoryID, commitID)
}

func (g *KVGraveler) ListBranches(ctx context.Context, repositoryID RepositoryID) (BranchIterator, error) {
	return g.db.ListBranches(ctx, repositoryID)
}

func (g *KVGraveler) DeleteBranch(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error {
	return g.db.DeleteBranch(ctx, repositoryID, branchID)
}

func (g *KVGraveler) GetStagingToken(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (*StagingToken, error) {
	return g.db.GetStagingToken(ctx, repositoryID, branchID)
}

func (g *KVGraveler) GetGarbageCollectionRules(ctx context.Context, repositoryID RepositoryID) (*GarbageCollectionRules, error) {
	return g.db.GetGarbageCollectionRules(ctx, repositoryID)
}

func (g *KVGraveler) SetGarbageCollectionRules(ctx context.Context, repositoryID RepositoryID, rules *GarbageCollectionRules) error {
	return g.db.SetGarbageCollectionRules(ctx, repositoryID, rules)
}

func (g *KVGraveler) SaveGarbageCollectionCommits(ctx context.Context, repositoryID RepositoryID, previousRunID string) (*GarbageCollectionRunMetadata, error) {
	return g.db.SaveGarbageCollectionCommits(ctx, repositoryID, previousRunID)
}

func (g *KVGraveler) GetBranchProtectionRules(ctx context.Context, repositoryID RepositoryID) (*BranchProtectionRules, error) {
	return g.db.GetBranchProtectionRules(ctx, repositoryID)
}

func (g *KVGraveler) DeleteBranchProtectionRule(ctx context.Context, repositoryID RepositoryID, pattern string) error {
	return g.db.DeleteBranchProtectionRule(ctx, repositoryID, pattern)
}

func (g *KVGraveler) CreateBranchProtectionRule(ctx context.Context, repositoryID RepositoryID, pattern string, blockedActions []BranchProtectionBlockedAction) error {
	return g.db.CreateBranchProtectionRule(ctx, repositoryID, pattern, blockedActions)
}

func (g *KVGraveler) Get(ctx context.Context, repositoryID RepositoryID, ref Ref, key Key) (*Value, error) {
	return g.db.Get(ctx, repositoryID, ref, key)
}

func (g *KVGraveler) Set(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key, value Value, writeConditions ...WriteConditionOption) error {
	return g.db.Set(ctx, repositoryID, branchID, key, value, writeConditions...)
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

func (g *KVGraveler) Delete(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	return g.db.Delete(ctx, repositoryID, branchID, key)
}

func (g *KVGraveler) List(ctx context.Context, repositoryID RepositoryID, ref Ref) (ValueIterator, error) {
	return g.db.List(ctx, repositoryID, ref)
}

func (g *KVGraveler) Commit(ctx context.Context, repositoryID RepositoryID, branchID BranchID, params CommitParams) (CommitID, error) {
	// TODO(issue 3569) - kv implementation optimistic lock commit
	var preRunID string
	var commit Commit
	var storageNamespace StorageNamespace
	res, err := g.db.branchLocker.MetadataUpdater(ctx, repositoryID, branchID, func() (interface{}, error) {
		isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repositoryID, branchID, BranchProtectionBlockedAction_COMMIT)
		if err != nil {
			return nil, err
		}
		if isProtected {
			return nil, ErrCommitToProtectedBranch
		}
		repo, err := g.GetRepository(ctx, repositoryID)
		if err != nil {
			return "", fmt.Errorf("get repository: %w", err)
		}
		storageNamespace = repo.StorageNamespace

		branch, err := g.db.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return "", fmt.Errorf("get branch: %w", err)
		}

		// fill commit information - use for pre-commit and after adding the commit information used by commit
		commit = NewCommit()

		if params.Date != nil {
			commit.CreationDate = time.Unix(*params.Date, 0)
		}
		commit.Committer = params.Committer
		commit.Message = params.Message
		commit.Metadata = params.Metadata
		if branch.CommitID != "" {
			commit.Parents = CommitParents{branch.CommitID}
		}

		if params.SourceMetaRange != nil {
			empty, err := g.stagingEmpty(ctx, branch)
			if err != nil {
				return nil, fmt.Errorf("checking empty branch: %w", err)
			}
			if !empty {
				return nil, ErrCommitMetaRangeDirtyBranch
			}
		}

		preRunID = g.hooks.NewRunID()
		err = g.hooks.PreCommitHook(ctx, HookRecord{
			RunID:            preRunID,
			EventType:        EventTypePreCommit,
			SourceRef:        branchID.Ref(),
			RepositoryID:     repositoryID,
			StorageNamespace: storageNamespace,
			BranchID:         branchID,
			Commit:           commit,
		})
		if err != nil {
			return "", &HookAbortError{
				EventType: EventTypePreCommit,
				RunID:     preRunID,
				Err:       err,
			}
		}

		var branchMetaRangeID MetaRangeID
		var parentGeneration int
		if branch.CommitID != "" {
			branchCommit, err := g.RefManager.GetCommit(ctx, repositoryID, branch.CommitID)
			if err != nil {
				return "", fmt.Errorf("get commit: %w", err)
			}
			branchMetaRangeID = branchCommit.MetaRangeID
			parentGeneration = branchCommit.Generation
		}
		commit.Generation = parentGeneration + 1
		if params.SourceMetaRange != nil {
			commit.MetaRangeID = *params.SourceMetaRange
		} else {
			changes, err := g.StagingManager.List(ctx, branch.StagingToken, ListingMaxBatchSize)
			if err != nil {
				return "", fmt.Errorf("staging list: %w", err)
			}
			defer changes.Close()

			commit.MetaRangeID, _, err = g.CommittedManager.Commit(ctx, storageNamespace, branchMetaRangeID, changes)
			if err != nil {
				return "", fmt.Errorf("commit: %w", err)
			}
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
	newCommitID := res.(CommitID)
	postRunID := g.hooks.NewRunID()
	err = g.hooks.PostCommitHook(ctx, HookRecord{
		EventType:        EventTypePostCommit,
		RunID:            postRunID,
		RepositoryID:     repositoryID,
		StorageNamespace: storageNamespace,
		SourceRef:        res.(CommitID).Ref(),
		BranchID:         branchID,
		Commit:           commit,
		CommitID:         newCommitID,
		PreRunID:         preRunID,
	})
	if err != nil {
		g.log.WithError(err).
			WithField("run_id", postRunID).
			WithField("pre_run_id", preRunID).
			Error("Post-commit hook failed")
	}
	return newCommitID, nil
}

func newStagingToken(repositoryID RepositoryID, branchID BranchID) StagingToken {
	v := strings.Join([]string{repositoryID.String(), branchID.String(), uuid.New().String()}, "-")
	return StagingToken(v)
}

func validateCommitParent(ctx context.Context, repositoryID RepositoryID, commit Commit, manager RefManager) (CommitID, error) {
	if len(commit.Parents) > 1 {
		return "", ErrMultipleParents
	}
	if len(commit.Parents) == 0 {
		return "", nil
	}

	parentCommitID := commit.Parents[0]
	_, err := manager.GetCommit(ctx, repositoryID, parentCommitID)
	if err != nil {
		return "", fmt.Errorf("get parent commit %s: %w", parentCommitID, err)
	}
	return parentCommitID, nil
}

func CommitExists(ctx context.Context, repositoryID RepositoryID, commitID CommitID, manager RefManager) (bool, error) {
	_, err := manager.GetCommit(ctx, repositoryID, commitID)
	if err == nil {
		// commit already exists
		return true, nil
	}
	if !errors.Is(err, ErrCommitNotFound) {
		return false, fmt.Errorf("getting commit %s: %w", commitID, err)
	}
	return false, nil
}

func (g *KVGraveler) AddCommitToBranchHead(ctx context.Context, repositoryID RepositoryID, branchID BranchID, commit Commit) (CommitID, error) {
	// TODO(issue 3569) - kv implementation optimistic lock commit
	res, err := g.db.branchLocker.MetadataUpdater(ctx, repositoryID, branchID, func() (interface{}, error) {
		// parentCommitID should always match the HEAD of the branch.
		// Empty parentCommitID matches first commit of the branch.
		parentCommitID, err := validateCommitParent(ctx, repositoryID, commit, g.RefManager)
		if err != nil {
			return nil, err
		}

		branch, err := g.db.GetBranch(ctx, repositoryID, branchID)
		if err != nil {
			return nil, err
		}
		if branch.CommitID != parentCommitID {
			return nil, ErrCommitNotHeadBranch
		}

		// check if commit already exists.
		commitID := CommitID(ident.NewHexAddressProvider().ContentAddress(commit))
		if exists, err := CommitExists(ctx, repositoryID, commitID, g.RefManager); err != nil {
			return nil, err
		} else if exists {
			return commitID, nil
		}

		commitID, err = g.addCommitNoLock(ctx, repositoryID, commit)
		if err != nil {
			return nil, fmt.Errorf("adding commit: %w", err)
		}
		_, err = g.db.updateBranchNoLock(ctx, repositoryID, branchID, Ref(commitID))
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

func (g *KVGraveler) AddCommit(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error) {
	// TODO(issue 3569) - kv implementation optimistic lock commit
	// at least a single parent must exists
	if len(commit.Parents) == 0 {
		return "", ErrAddCommitNoParent
	}
	_, err := validateCommitParent(ctx, repositoryID, commit, g.RefManager)
	if err != nil {
		return "", err
	}

	// check if commit already exists.
	commitID := CommitID(ident.NewHexAddressProvider().ContentAddress(commit))
	if exists, err := CommitExists(ctx, repositoryID, commitID, g.RefManager); err != nil {
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
func (g *KVGraveler) addCommitNoLock(ctx context.Context, repositoryID RepositoryID, commit Commit) (CommitID, error) {
	// TODO(issue 3569) - kv implementation optimistic lock commit
	repo, err := g.GetRepository(ctx, repositoryID)
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

func (g *KVGraveler) stagingEmpty(ctx context.Context, branch *Branch) (bool, error) {
	// TODO(eden) - change to the kv branch implementation version
	stIt, err := g.StagingManager.List(ctx, branch.StagingToken, 1)
	if err != nil {
		return false, fmt.Errorf("staging list (token %s): %w", branch.StagingToken, err)
	}

	defer stIt.Close()

	if stIt.Next() {
		return false, nil
	}

	return true, nil
}

func (g *KVGraveler) Reset(ctx context.Context, repositoryID RepositoryID, branchID BranchID) error {
	return g.db.Reset(ctx, repositoryID, branchID)
}

func (g *KVGraveler) ResetKey(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	return g.db.ResetKey(ctx, repositoryID, branchID, key)
}

func (g *KVGraveler) ResetPrefix(ctx context.Context, repositoryID RepositoryID, branchID BranchID, key Key) error {
	return g.db.ResetPrefix(ctx, repositoryID, branchID, key)
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
func (g *KVGraveler) Revert(ctx context.Context, repositoryID RepositoryID, branchID BranchID, ref Ref, parentNumber int, commitParams CommitParams) (CommitID, error) {
	return g.db.Revert(ctx, repositoryID, branchID, ref, parentNumber, commitParams)
}

func (g *KVGraveler) Merge(ctx context.Context, repositoryID RepositoryID, destination BranchID, source Ref, commitParams CommitParams, strategy string) (CommitID, error) {
	return g.db.Merge(ctx, repositoryID, destination, source, commitParams, strategy)
}

func (g *KVGraveler) DiffUncommitted(ctx context.Context, repositoryID RepositoryID, branchID BranchID) (DiffIterator, error) {
	return g.db.DiffUncommitted(ctx, repositoryID, branchID)
}

func (g *KVGraveler) Diff(ctx context.Context, repositoryID RepositoryID, left, right Ref) (DiffIterator, error) {
	return g.db.Diff(ctx, repositoryID, left, right)
}

func (g *KVGraveler) Compare(ctx context.Context, repositoryID RepositoryID, left, right Ref) (DiffIterator, error) {
	return g.db.Compare(ctx, repositoryID, left, right)
}

func (g *KVGraveler) SetHooksHandler(handler HooksHandler) {
	g.db.SetHooksHandler(handler)
	if handler == nil {
		g.hooks = &HooksNoOp{}
	} else {
		g.hooks = handler
	}
}

func (g *KVGraveler) LoadCommits(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) error {
	return g.db.LoadCommits(ctx, repositoryID, metaRangeID)
}

func (g *KVGraveler) LoadBranches(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) error {
	return g.db.LoadBranches(ctx, repositoryID, metaRangeID)
}

func (g *KVGraveler) LoadTags(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) error {
	return g.db.LoadTags(ctx, repositoryID, metaRangeID)
}

func (g *KVGraveler) GetMetaRange(ctx context.Context, repositoryID RepositoryID, metaRangeID MetaRangeID) (MetaRangeAddress, error) {
	return g.db.GetMetaRange(ctx, repositoryID, metaRangeID)
}

func (g *KVGraveler) GetRange(ctx context.Context, repositoryID RepositoryID, rangeID RangeID) (RangeAddress, error) {
	return g.db.GetRange(ctx, repositoryID, rangeID)
}

func (g *KVGraveler) DumpCommits(ctx context.Context, repositoryID RepositoryID) (*MetaRangeID, error) {
	return g.db.DumpCommits(ctx, repositoryID)
}

func (g *KVGraveler) DumpBranches(ctx context.Context, repositoryID RepositoryID) (*MetaRangeID, error) {
	return g.db.DumpBranches(ctx, repositoryID)
}

func (g *KVGraveler) DumpTags(ctx context.Context, repositoryID RepositoryID) (*MetaRangeID, error) {
	return g.db.DumpTags(ctx, repositoryID)
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
		Version:      int32(commit.Version),
		Id:           string(commit.CommitID),
		Committer:    commit.Committer,
		Message:      commit.Message,
		CreationDate: timestamppb.New(commit.CreationDate),
		MetaRangeId:  string(commit.MetaRangeID),
		Metadata:     commit.Metadata,
		Parents:      commit.Parents.AsStringSlice(),
		Generation:   int32(commit.Generation),
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

type GarbageCollectionManager interface {
	GetRules(ctx context.Context, storageNamespace StorageNamespace) (*GarbageCollectionRules, error)
	SaveRules(ctx context.Context, storageNamespace StorageNamespace, rules *GarbageCollectionRules) error

	SaveGarbageCollectionCommits(ctx context.Context, storageNamespace StorageNamespace, repositoryID RepositoryID, repo Repository, rules *GarbageCollectionRules, previouslyExpiredCommits []CommitID) (string, error)
	GetRunExpiredCommits(ctx context.Context, storageNamespace StorageNamespace, runID string) ([]CommitID, error)
	GetCommitsCSVLocation(runID string, sn StorageNamespace) (string, error)
	GetAddressesLocation(sn StorageNamespace) (string, error)
}

type ProtectedBranchesManager interface {
	// Add creates a rule for the given name pattern, blocking the given actions.
	// Returns ErrRuleAlreadyExists if there is already a rule for the given pattern.
	Add(ctx context.Context, repositoryID RepositoryID, branchNamePattern string, blockedActions []BranchProtectionBlockedAction) error
	// Delete deletes the rule for the given name pattern, or returns ErrRuleNotExists if there is no such rule.
	Delete(ctx context.Context, repositoryID RepositoryID, branchNamePattern string) error
	// Get returns the list of blocked actions for the given name pattern, or nil if no rule was defined for the pattern.
	Get(ctx context.Context, repositoryID RepositoryID, branchNamePattern string) ([]BranchProtectionBlockedAction, error)
	// GetRules returns all branch protection rules for the repository
	GetRules(ctx context.Context, repositoryID RepositoryID) (*BranchProtectionRules, error)
	// IsBlocked returns whether the action is blocked by any branch protection rule matching the given branch.
	IsBlocked(ctx context.Context, repositoryID RepositoryID, branchID BranchID, action BranchProtectionBlockedAction) (bool, error)
}
