package graveler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/rs/xid"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
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

	SettingsRelativeKey = "%s/settings/%s.json" // where the settings are saved relative to the storage namespace

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
//   ResolvedBranchModifier: branch indicator if resolved to a branch the latest commit, staging or none was specified.
//   CommitID: the commit ID of the branch head,  tag or specific hash.
//   StagingToken: empty if ResolvedBranchModifier is ResolvedBranchModifierCommitted.
//
type ResolvedRef struct {
	Type                   ReferenceType
	ResolvedBranchModifier ResolvedBranchModifier
	BranchRecord
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
	// Calculated by a hash function to all keys and values' identity.
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

// Key represents a logical path for a value
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
	// RepositoryState represents the state of the repository, only ACTIVE repository is considered a valid one.
	// other states represent in invalid temporary or terminal state
	State RepositoryState
	// InstanceUID identifies repository in a unique way. Since repositories with same name can be deleted and recreated
	// this field identifies the specific instance, and used in the KV store key path to store all the entities belonging
	// to this specific instantiation of repo with the given ID
	InstanceUID string
}

func NewRepository(storageNamespace StorageNamespace, defaultBranchID BranchID) Repository {
	return Repository{
		StorageNamespace: storageNamespace,
		CreationDate:     time.Now().UTC(),
		DefaultBranchID:  defaultBranchID,
		State:            RepositoryState_ACTIVE,
		InstanceUID:      NewRepoInstanceID(),
	}
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
	// SealedTokens - Staging tokens are appended to the front, this allows building the diff iterator easily
	SealedTokens []StagingToken
}

// BranchRecord holds BranchID with the associated Branch data
type BranchRecord struct {
	BranchID BranchID `db:"id"`
	*Branch
}

// BranchUpdateFunc Used to pass validation call back to ref manager for UpdateBranch flow
type BranchUpdateFunc func(*Branch) (*Branch, error)

// ValueUpdateFunc Used to pass validation call back to staging manager for UpdateValue flow
type ValueUpdateFunc func(*Value) (*Value, error)

// TagRecord holds TagID with the associated Tag data
type TagRecord struct {
	TagID    TagID `db:"id"`
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
	Get(ctx context.Context, repository *RepositoryRecord, ref Ref, key Key) (*Value, error)

	// GetByCommitID returns value from repository / commit by key and error if value does not exist
	GetByCommitID(ctx context.Context, repository *RepositoryRecord, commitID CommitID, key Key) (*Value, error)

	// Set stores value on repository / branch by key. nil value is a valid value for tombstone
	Set(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key, value Value, writeConditions ...WriteConditionOption) error

	// Delete value from repository / branch by key
	Delete(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error

	// List lists values on repository / ref
	List(ctx context.Context, repository *RepositoryRecord, ref Ref) (ValueIterator, error)
}

type VersionController interface {
	// GetRepository returns the Repository metadata object for the given RepositoryID
	GetRepository(ctx context.Context, repositoryID RepositoryID) (*RepositoryRecord, error)

	// CreateRepository stores a new Repository under RepositoryID with the given Branch as default branch
	CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*RepositoryRecord, error)

	// CreateBareRepository stores a new Repository under RepositoryID with no initial branch or commit
	CreateBareRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, defaultBranchID BranchID) (*RepositoryRecord, error)

	// ListRepositories returns iterator to scan repositories
	ListRepositories(ctx context.Context) (RepositoryIterator, error)

	// DeleteRepository deletes the repository
	DeleteRepository(ctx context.Context, repositoryID RepositoryID) error

	// CreateBranch creates branch on repository pointing to ref
	CreateBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref) (*Branch, error)

	// UpdateBranch updates branch on repository pointing to ref
	UpdateBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref) (*Branch, error)

	// GetBranch gets branch information by branch / repository id
	GetBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (*Branch, error)

	// GetTag gets tag's commit id
	GetTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) (*CommitID, error)

	// CreateTag creates tag on a repository pointing to a commit id
	CreateTag(ctx context.Context, repository *RepositoryRecord, tagID TagID, commitID CommitID) error

	// DeleteTag remove tag from a repository
	DeleteTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) error

	// ListTags lists tags on a repository
	ListTags(ctx context.Context, repository *RepositoryRecord) (TagIterator, error)

	// Log returns an iterator starting at commit ID up to repository root
	Log(ctx context.Context, repository *RepositoryRecord, commitID CommitID) (CommitIterator, error)

	// ListBranches lists branches on repositories
	ListBranches(ctx context.Context, repository *RepositoryRecord) (BranchIterator, error)

	// DeleteBranch deletes branch from repository
	DeleteBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error

	// Commit the staged data and returns a commit ID that references that change
	//   ErrNothingToCommit in case there is no data in stage
	Commit(ctx context.Context, repository *RepositoryRecord, branchID BranchID, commitParams CommitParams) (CommitID, error)

	// WriteMetaRangeByIterator accepts a ValueIterator and writes the entire iterator to a new MetaRange
	// and returns the result ID.
	WriteMetaRangeByIterator(ctx context.Context, repository *RepositoryRecord, it ValueIterator) (*MetaRangeID, error)

	// AddCommitToBranchHead creates a commit in the branch from the given pre-existing tree.
	// Returns ErrMetaRangeNotFound if the referenced metaRangeID doesn't exist.
	// Returns ErrCommitNotHeadBranch if the branch is no longer referencing to the parentCommit
	AddCommitToBranchHead(ctx context.Context, repository *RepositoryRecord, branchID BranchID, commit Commit) (CommitID, error)

	// AddCommit creates a dangling (no referencing branch) commit in the repo from the pre-existing commit.
	// Returns ErrMetaRangeNotFound if the referenced metaRangeID doesn't exist.
	AddCommit(ctx context.Context, repository *RepositoryRecord, commit Commit) (CommitID, error)

	// GetCommit returns the Commit metadata object for the given CommitID
	GetCommit(ctx context.Context, repository *RepositoryRecord, commitID CommitID) (*Commit, error)

	// Dereference returns the resolved ref information based on 'ref' reference
	Dereference(ctx context.Context, repository *RepositoryRecord, ref Ref) (*ResolvedRef, error)

	// ParseRef returns parsed 'ref' information as raw reference
	ParseRef(ref Ref) (RawRef, error)

	// ResolveRawRef returns the ResolvedRef matching the given RawRef
	ResolveRawRef(ctx context.Context, repository *RepositoryRecord, rawRef RawRef) (*ResolvedRef, error)

	// Reset throws all staged data on the repository / branch
	Reset(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error

	// ResetKey throws all staged data under the specified key on the repository / branch
	ResetKey(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error

	// ResetPrefix throws all staged data starting with the given prefix on the repository / branch
	ResetPrefix(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error

	// Revert creates a reverse patch to the commit given as 'ref', and applies it as a new commit on the given branch.
	Revert(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref, parentNumber int, commitParams CommitParams) (CommitID, error)

	// Merge merges 'source' into 'destination' and returns the commit id for the created merge commit.
	Merge(ctx context.Context, repository *RepositoryRecord, destination BranchID, source Ref, commitParams CommitParams, strategy string) (CommitID, error)

	// DiffUncommitted returns iterator to scan the changes made on the branch
	DiffUncommitted(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (DiffIterator, error)

	// Diff returns the changes between 'left' and 'right' ref.
	// This is similar to a two-dot (left..right) diff in git.
	Diff(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error)

	// Compare returns the difference between the commit where 'left' was last synced into 'right', and the most recent commit of `right`.
	// This is similar to a three-dot (from...to) diff in git.
	Compare(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error)

	// SetHooksHandler set handler for all graveler hooks
	SetHooksHandler(handler HooksHandler)

	// GetStagingToken returns the token identifying current staging for branchID of
	// repositoryID.
	GetStagingToken(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (*StagingToken, error)

	GetGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord) (*GarbageCollectionRules, error)

	SetGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord, rules *GarbageCollectionRules) error

	// SaveGarbageCollectionCommits saves the sets of active and expired commits, according to the branch rules for garbage collection.
	// Returns
	//	- run id which can later be used to retrieve the set of commits.
	//	- location where the expired/active commit information was saved
	//	- location where the information of addresses to be removed should be saved
	// If a previousRunID is specified, commits that were already expired and their ancestors will not be considered as expired/active.
	// Note: Ancestors of previously expired commits may still be considered if they can be reached from a non-expired commit.
	SaveGarbageCollectionCommits(ctx context.Context, repository *RepositoryRecord, previousRunID string) (garbageCollectionRunMetadata *GarbageCollectionRunMetadata, err error)

	// GetBranchProtectionRules return all branch protection rules for the repository
	GetBranchProtectionRules(ctx context.Context, repository *RepositoryRecord) (*BranchProtectionRules, error)

	// DeleteBranchProtectionRule deletes the branch protection rule for the given pattern,
	// or return ErrRuleNotExists if no such rule exists.
	DeleteBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string) error

	// CreateBranchProtectionRule creates a rule for the given name pattern,
	// or returns ErrRuleAlreadyExists if there is already a rule for the pattern.
	CreateBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string, blockedActions []BranchProtectionBlockedAction) error
}

// Plumbing includes commands for fiddling more directly with graveler implementation
// internals.
type Plumbing interface {
	// GetMetaRange returns information where metarangeID is stored.
	GetMetaRange(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) (MetaRangeAddress, error)
	// GetRange returns information where rangeID is stored.
	GetRange(ctx context.Context, repository *RepositoryRecord, rangeID RangeID) (RangeAddress, error)
	// WriteRange creates a new Range from the iterator values.
	// Keeps Range closing logic, so might not flush all values to the range.
	WriteRange(ctx context.Context, repository *RepositoryRecord, it ValueIterator) (*RangeInfo, error)
	// WriteMetaRange creates a new MetaRange from the given Ranges.
	WriteMetaRange(ctx context.Context, repository *RepositoryRecord, ranges []*RangeInfo) (*MetaRangeInfo, error)
}

type Dumper interface {
	// DumpCommits iterates through all commits and dumps them in Graveler format
	DumpCommits(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error)

	// DumpBranches iterates through all branches and dumps them in Graveler format
	DumpBranches(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error)

	// DumpTags iterates through all tags and dumps them in Graveler format
	DumpTags(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error)
}

type Loader interface {
	// LoadCommits iterates through all commits in Graveler format and loads them into repositoryID
	LoadCommits(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error

	// LoadBranches iterates through all branches in Graveler format and loads them into repositoryID
	LoadBranches(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error

	// LoadTags iterates through all tags in Graveler format and loads them into repositoryID
	LoadTags(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error
}

// Internal structures used by Graveler
// xxxIterator used as follows:
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
	GetRepository(ctx context.Context, repositoryID RepositoryID) (*RepositoryRecord, error)

	// CreateRepository stores a new Repository under RepositoryID with the given Branch as default branch
	CreateRepository(ctx context.Context, repositoryID RepositoryID, repository Repository) (*RepositoryRecord, error)

	// CreateBareRepository stores a new repository under RepositoryID without creating an initial commit and branch
	CreateBareRepository(ctx context.Context, repositoryID RepositoryID, repository Repository) (*RepositoryRecord, error)

	// ListRepositories lists repositories
	ListRepositories(ctx context.Context) (RepositoryIterator, error)

	// DeleteRepository deletes the repository
	DeleteRepository(ctx context.Context, repositoryID RepositoryID) error

	// ParseRef returns parsed 'ref' information as RawRef
	ParseRef(ref Ref) (RawRef, error)

	// ResolveRawRef returns the ResolvedRef matching the given RawRef
	ResolveRawRef(ctx context.Context, repository *RepositoryRecord, rawRef RawRef) (*ResolvedRef, error)

	// GetBranch returns the Branch metadata object for the given BranchID
	GetBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (*Branch, error)

	// CreateBranch creates a branch with the given id and Branch metadata
	CreateBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, branch Branch) error

	// SetBranch points the given BranchID at the given Branch metadata
	SetBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, branch Branch) error

	// BranchUpdate Conditional set of branch with validation callback
	BranchUpdate(ctx context.Context, repository *RepositoryRecord, branchID BranchID, f BranchUpdateFunc) error

	// DeleteBranch deletes the branch
	DeleteBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error

	// ListBranches lists branches
	ListBranches(ctx context.Context, repository *RepositoryRecord) (BranchIterator, error)

	// GCBranchIterator TODO (niro): Remove when DB implementation is deleted
	// GCBranchIterator temporary WA to support both DB and KV GC BranchIterator, which iterates over branches by order of commit ID
	GCBranchIterator(ctx context.Context, repository *RepositoryRecord) (BranchIterator, error)

	// GetTag returns the Tag metadata object for the given TagID
	GetTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) (*CommitID, error)

	// CreateTag create a given tag pointing to a commit
	CreateTag(ctx context.Context, repository *RepositoryRecord, tagID TagID, commitID CommitID) error

	// DeleteTag deletes the tag
	DeleteTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) error

	// ListTags lists tags
	ListTags(ctx context.Context, repository *RepositoryRecord) (TagIterator, error)

	// GetCommit returns the Commit metadata object for the given CommitID.
	GetCommit(ctx context.Context, repository *RepositoryRecord, commitID CommitID) (*Commit, error)

	// GetCommitByPrefix returns the Commit metadata object for the given prefix CommitID.
	// if more than 1 commit starts with the ID prefix returns error
	GetCommitByPrefix(ctx context.Context, repository *RepositoryRecord, prefix CommitID) (*Commit, error)

	// AddCommit stores the Commit object, returning its ID
	AddCommit(ctx context.Context, repository *RepositoryRecord, commit Commit) (CommitID, error)

	// RemoveCommit deletes commit from store - used for repository cleanup
	RemoveCommit(ctx context.Context, repository *RepositoryRecord, commitID CommitID) error

	// FindMergeBase returns the merge-base for the given CommitIDs
	// see: https://git-scm.com/docs/git-merge-base
	// and internally: https://github.com/treeverse/lakeFS/blob/09954804baeb36ada74fa17d8fdc13a38552394e/index/dag/commits.go
	FindMergeBase(ctx context.Context, repository *RepositoryRecord, commitIDs ...CommitID) (*Commit, error)

	// Log returns an iterator starting at commit ID up to repository root
	Log(ctx context.Context, repository *RepositoryRecord, commitID CommitID) (CommitIterator, error)

	// ListCommits returns an iterator over all known commits, ordered by their commit ID
	ListCommits(ctx context.Context, repository *RepositoryRecord) (CommitIterator, error)

	// GCCommitIterator TODO (niro): Remove when DB implementation is deleted
	// GCCommitIterator temporary WA to support both DB and KV GC CommitIterator
	GCCommitIterator(ctx context.Context, repository *RepositoryRecord) (CommitIterator, error)
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

	// Update updates a (possibly nil) value under the given staging token and key.
	Update(ctx context.Context, st StagingToken, key Key, updateFunc ValueUpdateFunc) error

	// List returns a ValueIterator for the given staging token
	List(ctx context.Context, st StagingToken, batchSize int) (ValueIterator, error)

	// DropKey clears a value by staging token and key
	DropKey(ctx context.Context, st StagingToken, key Key) error

	// Drop clears the given staging area
	Drop(ctx context.Context, st StagingToken) error

	// DropAsync clears the given staging area eventually. Keys may still exist under the token
	// for a short period of time after the request was made.
	DropAsync(ctx context.Context, st StagingToken) error

	// DropByPrefix drops all keys starting with the given prefix, from the given staging area
	DropByPrefix(ctx context.Context, st StagingToken, prefix Key) error
}

// BranchLockerFunc callback function when branch is locked for operation (ex: writer or metadata updater)
type BranchLockerFunc func() (interface{}, error)

type BranchLocker interface {
	Writer(ctx context.Context, repository *RepositoryRecord, branchID BranchID, lockedFn BranchLockerFunc) (interface{}, error)
	MetadataUpdater(ctx context.Context, repository *RepositoryRecord, branchID BranchID, lockeFn BranchLockerFunc) (interface{}, error)
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

func (id StagingToken) String() string {
	return string(id)
}

func (id MetaRangeID) String() string {
	return string(id)
}

type KVGraveler struct {
	hooks                    HooksHandler
	CommittedManager         CommittedManager
	RefManager               RefManager
	StagingManager           StagingManager
	protectedBranchesManager ProtectedBranchesManager
	garbageCollectionManager GarbageCollectionManager
	log                      logging.Logger
}

func NewKVGraveler(committedManager CommittedManager, stagingManager StagingManager, refManager RefManager, gcManager GarbageCollectionManager, protectedBranchesManager ProtectedBranchesManager) *KVGraveler {
	return &KVGraveler{
		hooks:                    &HooksNoOp{},
		CommittedManager:         committedManager,
		RefManager:               refManager,
		StagingManager:           stagingManager,
		protectedBranchesManager: protectedBranchesManager,
		garbageCollectionManager: gcManager,

		log: logging.Default().WithField("service_name", "graveler_graveler"),
	}
}

func (g *KVGraveler) GetRepository(ctx context.Context, repositoryID RepositoryID) (*RepositoryRecord, error) {
	return g.RefManager.GetRepository(ctx, repositoryID)
}

func (g *KVGraveler) CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*RepositoryRecord, error) {
	_, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil && !errors.Is(err, ErrRepositoryNotFound) {
		return nil, err
	}

	repo := NewRepository(storageNamespace, branchID)
	repository, err := g.RefManager.CreateRepository(ctx, repositoryID, repo)
	if err != nil {
		return nil, err
	}
	return repository, nil
}

func (g *KVGraveler) CreateBareRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, defaultBranchID BranchID) (*RepositoryRecord, error) {
	_, err := g.RefManager.GetRepository(ctx, repositoryID)
	if err != nil && !errors.Is(err, ErrRepositoryNotFound) {
		return nil, err
	}

	repo := NewRepository(storageNamespace, defaultBranchID)
	repository, err := g.RefManager.CreateBareRepository(ctx, repositoryID, repo)
	if err != nil {
		return nil, err
	}
	return repository, nil
}

func (g *KVGraveler) ListRepositories(ctx context.Context) (RepositoryIterator, error) {
	return g.RefManager.ListRepositories(ctx)
}

func (g *KVGraveler) WriteRange(ctx context.Context, repository *RepositoryRecord, it ValueIterator) (*RangeInfo, error) {
	return g.CommittedManager.WriteRange(ctx, repository.StorageNamespace, it)
}

func (g *KVGraveler) WriteMetaRange(ctx context.Context, repository *RepositoryRecord, ranges []*RangeInfo) (*MetaRangeInfo, error) {
	return g.CommittedManager.WriteMetaRange(ctx, repository.StorageNamespace, ranges)
}

func (g *KVGraveler) WriteMetaRangeByIterator(ctx context.Context, repository *RepositoryRecord, it ValueIterator) (*MetaRangeID, error) {
	return g.CommittedManager.WriteMetaRangeByIterator(ctx, repository.StorageNamespace, it, nil)
}

func (g *KVGraveler) DeleteRepository(ctx context.Context, repositoryID RepositoryID) error {
	return g.RefManager.DeleteRepository(ctx, repositoryID)
}

func (g *KVGraveler) GetCommit(ctx context.Context, repository *RepositoryRecord, commitID CommitID) (*Commit, error) {
	return g.RefManager.GetCommit(ctx, repository, commitID)
}

func GenerateStagingToken(repositoryID RepositoryID, branchID BranchID) StagingToken {
	uid := uuid.New().String()
	return StagingToken(fmt.Sprintf("%s-%s:%s", repositoryID, branchID, uid))
}

func (g *KVGraveler) CreateBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref) (*Branch, error) {
	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, fmt.Errorf("source reference '%s': %w", ref, err)
	}
	if reference.ResolvedBranchModifier == ResolvedBranchModifierStaging {
		return nil, fmt.Errorf("source reference '%s': %w", ref, ErrCreateBranchNoCommit)
	}

	_, err = g.RefManager.GetBranch(ctx, repository, branchID)
	if err == nil {
		return nil, ErrBranchExists
	}
	if !errors.Is(err, ErrBranchNotFound) {
		return nil, err
	}

	newBranch := Branch{
		CommitID:     reference.CommitID,
		StagingToken: GenerateStagingToken(repository.RepositoryID, branchID),
		SealedTokens: make([]StagingToken, 0),
	}
	storageNamespace := repository.StorageNamespace
	preRunID := g.hooks.NewRunID()
	err = g.hooks.PreCreateBranchHook(ctx, HookRecord{
		RunID:            preRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePreCreateBranch,
		SourceRef:        ref,
		RepositoryID:     repository.RepositoryID,
		BranchID:         branchID,
		CommitID:         reference.CommitID,
	})
	if err != nil {
		return nil, &HookAbortError{
			EventType: EventTypePreCreateBranch,
			RunID:     preRunID,
			Err:       err,
		}
	}

	err = g.RefManager.CreateBranch(ctx, repository, branchID, newBranch)
	if err != nil {
		return nil, fmt.Errorf("set branch '%s' to '%v': %w", branchID, newBranch, err)
	}

	postRunID := g.hooks.NewRunID()
	g.hooks.PostCreateBranchHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostCreateBranch,
		SourceRef:        ref,
		RepositoryID:     repository.RepositoryID,
		BranchID:         branchID,
		CommitID:         reference.CommitID,
		PreRunID:         preRunID,
	})

	return &newBranch, nil
}

func (g *KVGraveler) UpdateBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref) (*Branch, error) {
	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, err
	}
	if reference.ResolvedBranchModifier == ResolvedBranchModifierStaging {
		return nil, fmt.Errorf("reference '%s': %w", ref, ErrDereferenceCommitWithStaging)
	}

	if err := g.prepareForCommitIDUpdate(ctx, repository, branchID); err != nil {
		return nil, err
	}

	var tokensToDrop []StagingToken
	var newBranch *Branch
	err = g.RefManager.BranchUpdate(ctx, repository, branchID, func(currBranch *Branch) (*Branch, error) {
		// TODO(Guys) return error only on conflicts, currently returns error for any changes on staging
		empty, err := g.isSealedEmpty(ctx, repository, currBranch)
		if err != nil {
			return nil, err
		}
		if !empty {
			return nil, ErrConflictFound
		}

		tokensToDrop = currBranch.SealedTokens
		currBranch.SealedTokens = []StagingToken{}
		currBranch.CommitID = reference.CommitID
		newBranch = currBranch
		return currBranch, nil
	})
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrConflictFound
		}
		return nil, err
	}
	g.dropTokens(ctx, tokensToDrop...)

	return newBranch, nil
}

// prepareForCommitIDUpdate will check that the staging area is empty,
// before adding a new staging token. It is best to use it before changing
// the branch HEAD as a preparation for deleting the staging area.
// see issue #3771 for more information on the algorithm used.
func (g *KVGraveler) prepareForCommitIDUpdate(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error {
	err := g.RefManager.BranchUpdate(ctx, repository, branchID, func(currBranch *Branch) (*Branch, error) {
		empty, err := g.isStagingEmpty(ctx, repository, currBranch)
		if err != nil {
			return nil, err
		}
		if !empty {
			return nil, ErrConflictFound
		}

		currBranch.SealedTokens = append([]StagingToken{currBranch.StagingToken}, currBranch.SealedTokens...)
		currBranch.StagingToken = GenerateStagingToken(repository.RepositoryID, branchID)
		return currBranch, nil
	})
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrConflictFound
		}
		return err
	}
	return nil
}

func (g *KVGraveler) GetBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (*Branch, error) {
	return g.RefManager.GetBranch(ctx, repository, branchID)
}

func (g *KVGraveler) GetTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) (*CommitID, error) {
	return g.RefManager.GetTag(ctx, repository, tagID)
}

func (g *KVGraveler) CreateTag(ctx context.Context, repository *RepositoryRecord, tagID TagID, commitID CommitID) error {
	storageNamespace := repository.StorageNamespace

	// Check that Tag doesn't exist before running hook - Non-Atomic operation
	_, err := g.RefManager.GetTag(ctx, repository, tagID)
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
		RepositoryID:     repository.RepositoryID,
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

	err = g.RefManager.CreateTag(ctx, repository, tagID, commitID)
	if err != nil {
		return err
	}

	postRunID := g.hooks.NewRunID()
	g.hooks.PostCreateTagHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostCreateTag,
		RepositoryID:     repository.RepositoryID,
		CommitID:         commitID,
		SourceRef:        commitID.Ref(),
		TagID:            tagID,
		PreRunID:         preRunID,
	})

	return nil
}

func (g *KVGraveler) DeleteTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) error {
	storageNamespace := repository.StorageNamespace

	// Sanity check that Tag exists before running hook.
	commitID, err := g.RefManager.GetTag(ctx, repository, tagID)
	if err != nil {
		return err
	}

	preRunID := g.hooks.NewRunID()
	err = g.hooks.PreDeleteTagHook(ctx, HookRecord{
		RunID:            preRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePreDeleteTag,
		RepositoryID:     repository.RepositoryID,
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

	err = g.RefManager.DeleteTag(ctx, repository, tagID)
	if err != nil {
		return err
	}

	postRunID := g.hooks.NewRunID()
	g.hooks.PostDeleteTagHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostDeleteTag,
		RepositoryID:     repository.RepositoryID,
		SourceRef:        commitID.Ref(),
		CommitID:         *commitID,
		TagID:            tagID,
		PreRunID:         preRunID,
	})
	return nil
}

func (g *KVGraveler) ListTags(ctx context.Context, repository *RepositoryRecord) (TagIterator, error) {
	return g.RefManager.ListTags(ctx, repository)
}

func (g *KVGraveler) Dereference(ctx context.Context, repository *RepositoryRecord, ref Ref) (*ResolvedRef, error) {
	rawRef, err := g.ParseRef(ref)
	if err != nil {
		return nil, err
	}
	return g.ResolveRawRef(ctx, repository, rawRef)
}

func (g *KVGraveler) ParseRef(ref Ref) (RawRef, error) {
	return g.RefManager.ParseRef(ref)
}

func (g *KVGraveler) ResolveRawRef(ctx context.Context, repository *RepositoryRecord, rawRef RawRef) (*ResolvedRef, error) {
	return g.RefManager.ResolveRawRef(ctx, repository, rawRef)
}

func (g *KVGraveler) Log(ctx context.Context, repository *RepositoryRecord, commitID CommitID) (CommitIterator, error) {
	return g.RefManager.Log(ctx, repository, commitID)
}

func (g *KVGraveler) ListBranches(ctx context.Context, repository *RepositoryRecord) (BranchIterator, error) {
	return g.RefManager.ListBranches(ctx, repository)
}

func (g *KVGraveler) DeleteBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error {
	if repository.DefaultBranchID == branchID {
		return ErrDeleteDefaultBranch
	}
	branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
	if err != nil {
		return err
	}

	commitID := branch.CommitID
	storageNamespace := repository.StorageNamespace
	preRunID := g.hooks.NewRunID()
	preHookRecord := HookRecord{
		RunID:            preRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePreDeleteBranch,
		RepositoryID:     repository.RepositoryID,
		SourceRef:        commitID.Ref(),
		BranchID:         branchID,
	}
	err = g.hooks.PreDeleteBranchHook(ctx, preHookRecord)
	if err != nil {
		return &HookAbortError{
			EventType: EventTypePreDeleteBranch,
			RunID:     preRunID,
			Err:       err,
		}
	}

	// Delete branch first - afterwards remove tokens
	err = g.RefManager.DeleteBranch(ctx, repository, branchID)
	if err != nil { // Don't perform post action hook if operation finished with error
		return err
	}

	tokens := branch.SealedTokens
	tokens = append(tokens, branch.StagingToken)
	g.dropTokens(ctx, tokens...)

	postRunID := g.hooks.NewRunID()
	g.hooks.PostDeleteBranchHook(ctx, HookRecord{
		RunID:            postRunID,
		StorageNamespace: storageNamespace,
		EventType:        EventTypePostDeleteBranch,
		RepositoryID:     repository.RepositoryID,
		SourceRef:        commitID.Ref(),
		BranchID:         branchID,
		PreRunID:         preRunID,
	})

	return nil
}

func (g *KVGraveler) GetStagingToken(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (*StagingToken, error) {
	branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
	if err != nil {
		return nil, err
	}
	return &branch.StagingToken, nil
}

func (g *KVGraveler) getGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord) (*GarbageCollectionRules, error) {
	return g.garbageCollectionManager.GetRules(ctx, repository.StorageNamespace)
}

func (g *KVGraveler) GetGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord) (*GarbageCollectionRules, error) {
	return g.getGarbageCollectionRules(ctx, repository)
}

func (g *KVGraveler) SetGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord, rules *GarbageCollectionRules) error {
	return g.garbageCollectionManager.SaveRules(ctx, repository.StorageNamespace, rules)
}

func (g *KVGraveler) SaveGarbageCollectionCommits(ctx context.Context, repository *RepositoryRecord, previousRunID string) (*GarbageCollectionRunMetadata, error) {
	rules, err := g.getGarbageCollectionRules(ctx, repository)
	if err != nil {
		return nil, fmt.Errorf("get gc rules: %w", err)
	}
	previouslyExpiredCommits, err := g.garbageCollectionManager.GetRunExpiredCommits(ctx, repository.StorageNamespace, previousRunID)
	if err != nil {
		return nil, fmt.Errorf("get expired commits from previous run: %w", err)
	}

	runID, err := g.garbageCollectionManager.SaveGarbageCollectionCommits(ctx, repository, rules, previouslyExpiredCommits)
	if err != nil {
		return nil, fmt.Errorf("save garbage collection commits: %w", err)
	}
	commitsLocation, err := g.garbageCollectionManager.GetCommitsCSVLocation(runID, repository.StorageNamespace)
	if err != nil {
		return nil, err
	}
	addressLocation, err := g.garbageCollectionManager.GetAddressesLocation(repository.StorageNamespace)
	if err != nil {
		return nil, err
	}

	return &GarbageCollectionRunMetadata{
		RunId:              runID,
		CommitsCsvLocation: commitsLocation,
		AddressLocation:    addressLocation,
	}, err
}

func (g *KVGraveler) GetBranchProtectionRules(ctx context.Context, repository *RepositoryRecord) (*BranchProtectionRules, error) {
	return g.protectedBranchesManager.GetRules(ctx, repository)
}

func (g *KVGraveler) DeleteBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string) error {
	return g.protectedBranchesManager.Delete(ctx, repository, pattern)
}

func (g *KVGraveler) CreateBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string, blockedActions []BranchProtectionBlockedAction) error {
	return g.protectedBranchesManager.Add(ctx, repository, pattern, blockedActions)
}

// getFromStagingArea returns the most updated value of a given key in a branch staging area.
// Iterate over all tokens - staging + sealed in order of last modified. First appearance of key represents the latest update
// TODO: in most cases it is used by Get flow, assuming that usually the key will be found in committed we need to parallelize the get from tokens
func (g *KVGraveler) getFromStagingArea(ctx context.Context, b *Branch, key Key) (*Value, error) {
	if b.StagingToken == "" {
		return nil, ErrNotFound
	}
	tokens := []StagingToken{b.StagingToken}
	tokens = append(tokens, b.SealedTokens...)
	for _, st := range tokens {
		value, err := g.StagingManager.Get(ctx, st, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) { // key not found on staging token, advance to the next one
				continue
			}
			return nil, err // Unexpected error
		}
		return value, nil // Found the latest update of the given Key
	}
	return nil, ErrNotFound // Key not in staging area
}

func (g *KVGraveler) Get(ctx context.Context, repository *RepositoryRecord, ref Ref, key Key) (*Value, error) {
	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, err
	}

	if reference.StagingToken != "" {
		// try to get from staging, if not found proceed to committed
		value, err := g.getFromStagingArea(ctx, reference.Branch, key)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if err == nil {
			if value == nil {
				// tombstone - the entry was deleted on the branch => doesn't exist
				return nil, ErrNotFound
			}
			return value, nil
		}
	}

	// If key is not found in staging area (or reference is not a branch), return the key from committed
	commitID := reference.CommitID
	commit, err := g.RefManager.GetCommit(ctx, repository, commitID)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Get(ctx, repository.StorageNamespace, commit.MetaRangeID, key)
}

func (g *KVGraveler) GetByCommitID(ctx context.Context, repository *RepositoryRecord, commitID CommitID, key Key) (*Value, error) {
	// If key is not found in staging area (or reference is not a branch), return the key from committed
	commit, err := g.RefManager.GetCommit(ctx, repository, commitID)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Get(ctx, repository.StorageNamespace, commit.MetaRangeID, key)
}

func (g *KVGraveler) Set(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key, value Value, writeConditions ...WriteConditionOption) error {
	isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
	if err != nil {
		return err
	}
	if isProtected {
		return ErrWriteToProtectedBranch
	}

	setFunc := func(branch *Branch) error {
		writeCondition := &WriteCondition{}
		for _, cond := range writeConditions {
			cond(writeCondition)
		}

		if !writeCondition.IfAbsent {
			return g.StagingManager.Set(ctx, branch.StagingToken, key, &value, !writeCondition.IfAbsent)
		}

		// check if the given key exist in the branch first
		_, err := g.Get(ctx, repository, Ref(branchID), key)
		if err == nil {
			// we got a key here already!
			return ErrPreconditionFailed
		}
		if !errors.Is(err, ErrNotFound) {
			// another error occurred!
			return err
		}

		return g.StagingManager.Update(ctx, branch.StagingToken, key, func(v *Value) (*Value, error) {
			if v == nil || v.Identity == nil {
				// value doesn't exist or is a tombstone
				return &value, nil
			}
			return nil, ErrPreconditionFailed
		})
	}

	return g.safeBranchWrite(ctx, g.log.WithField("key", key).WithField("operation", "set"), repository, branchID, setFunc)
}

// safeBranchWrite is a helper function that wraps a branch write operation with validation that the staging token
// didn't change while writing to the branch.
func (g *KVGraveler) safeBranchWrite(ctx context.Context, log logging.Logger, repository *RepositoryRecord, branchID BranchID, stagingOperation func(branch *Branch) error) error {
	// setTries is the number of times to repeat the set operation if the staging token changed
	const setTries = 3

	var try int
	for try = 0; try < setTries; try++ {
		branch, err := g.GetBranch(ctx, repository, branchID)
		if err != nil {
			return err
		}
		startToken := branch.StagingToken

		if err = stagingOperation(branch); err != nil {
			return err
		}

		// Checking if the token has changed.
		// If it changed, we need to write the changes to the branch's new staging token
		branch, err = g.GetBranch(ctx, repository, branchID)
		if err != nil {
			return err
		}
		endToken := branch.StagingToken
		if startToken == endToken {
			break
		} else {
			// we got a new token, try again
			log.WithField("try", try+1).
				WithField("startToken", startToken).
				WithField("endToken", endToken).
				Info("Retrying Set")
		}
	}
	if try == setTries {
		return fmt.Errorf("safe branch write: %w", ErrTooManyTries)
	}
	return nil
}

func (g *KVGraveler) Delete(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error {
	isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
	if err != nil {
		return err
	}
	if isProtected {
		return ErrWriteToProtectedBranch
	}

	deleteEntry := func(branch *Branch) error {
		commit, err := g.RefManager.GetCommit(ctx, repository, branch.CommitID)
		if err != nil {
			return err
		}

		// check key in committed - do we need tombstone?
		foundInCommitted := false
		_, err = g.CommittedManager.Get(ctx, repository.StorageNamespace, commit.MetaRangeID, key)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				// unknown error
				return fmt.Errorf("reading from committed: %w", err)
			}
		} else {
			foundInCommitted = true
		}

		foundInStaging := true
		val, err := g.getFromStagingArea(ctx, branch, key)
		if err != nil {
			if !errors.Is(err, ErrNotFound) {
				// unknown error
				return fmt.Errorf("reading from staging: %w", err)
			}
			foundInStaging = false
		} else if val == nil {
			// found tombstone in staging, return ErrNotFound
			return ErrNotFound
		}

		if foundInCommitted || foundInStaging {
			return g.StagingManager.Set(ctx, branch.StagingToken, key, nil, true)
		}

		// key is nowhere to be found, return ErrNotFound
		return ErrNotFound
	}

	return g.safeBranchWrite(ctx, g.log.WithField("key", key).WithField("operation", "delete"),
		repository, branchID, deleteEntry)
}

// listStagingArea Returns an iterator which is an aggregation of all changes on all the branch's staging area (staging + sealed)
// for each key in the staging area it will return the latest update for that key (the value that appears in the newest token)
func (g *KVGraveler) listStagingArea(ctx context.Context, b *Branch) (ValueIterator, error) {
	if b.StagingToken == "" {
		return nil, ErrNotFound
	}
	it, err := g.StagingManager.List(ctx, b.StagingToken, 1)
	if err != nil {
		return nil, err
	}

	if len(b.SealedTokens) == 0 { // Only staging token exists -> return its iterator
		return it, nil
	}

	itrs, err := g.listSealedTokens(ctx, b)
	if err != nil {
		it.Close()
		return nil, err
	}
	itrs = append([]ValueIterator{it}, itrs...)
	return NewCombinedIterator(itrs...), nil
}

func (g *KVGraveler) listSealedTokens(ctx context.Context, b *Branch) ([]ValueIterator, error) {
	itrs := make([]ValueIterator, 0, len(b.SealedTokens))
	for _, st := range b.SealedTokens {
		it, err := g.StagingManager.List(ctx, st, 1)
		if err != nil {
			for _, it = range itrs {
				it.Close()
			}
			return nil, err
		}
		itrs = append(itrs, it)
	}
	return itrs, nil
}

func (g *KVGraveler) sealedTokensIterator(ctx context.Context, b *Branch) (ValueIterator, error) {
	itrs, err := g.listSealedTokens(ctx, b)
	if err != nil {
		return nil, err
	}
	if len(itrs) == 0 {
		return nil, ErrNoChanges
	}

	changes := NewCombinedIterator(itrs...)
	return changes, nil
}

func (g *KVGraveler) List(ctx context.Context, repository *RepositoryRecord, ref Ref) (ValueIterator, error) {
	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, err
	}
	var metaRangeID MetaRangeID
	if reference.CommitID != "" {
		commit, err := g.RefManager.GetCommit(ctx, repository, reference.CommitID)
		if err != nil {
			return nil, err
		}
		metaRangeID = commit.MetaRangeID
	}

	listing, err := g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
	if err != nil {
		return nil, err
	}
	if reference.StagingToken != "" {
		stagingList, err := g.listStagingArea(ctx, reference.BranchRecord.Branch)
		if err != nil {
			listing.Close()
			return nil, err
		}
		listing = NewFilterTombstoneIterator(NewCombinedIterator(stagingList, listing))
	}

	return listing, nil
}

func (g *KVGraveler) Commit(ctx context.Context, repository *RepositoryRecord, branchID BranchID, params CommitParams) (CommitID, error) {
	var preRunID string
	var commit Commit
	var newCommitID CommitID
	var storageNamespace StorageNamespace
	var sealedToDrop []StagingToken

	isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_COMMIT)
	if err != nil {
		return "", err
	}
	if isProtected {
		return "", ErrCommitToProtectedBranch
	}
	storageNamespace = repository.StorageNamespace

	err = g.RefManager.BranchUpdate(ctx, repository, branchID, func(branch *Branch) (*Branch, error) {
		if params.SourceMetaRange != nil {
			empty, err := g.isStagingEmpty(ctx, repository, branch)
			if err != nil {
				return nil, fmt.Errorf("checking empty branch: %w", err)
			}
			if !empty {
				return nil, ErrCommitMetaRangeDirtyBranch
			}
		}
		branch.SealedTokens = append([]StagingToken{branch.StagingToken}, branch.SealedTokens...)
		branch.StagingToken = GenerateStagingToken(repository.RepositoryID, branchID)
		return branch, nil
	})
	if err != nil {
		return "", err
	}

	err = g.retryBranchUpdate(ctx, repository, branchID, func(branch *Branch) (*Branch, error) {
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

		preRunID = g.hooks.NewRunID()
		err = g.hooks.PreCommitHook(ctx, HookRecord{
			RunID:            preRunID,
			EventType:        EventTypePreCommit,
			SourceRef:        branchID.Ref(),
			RepositoryID:     repository.RepositoryID,
			StorageNamespace: storageNamespace,
			BranchID:         branchID,
			Commit:           commit,
		})
		if err != nil {
			return nil, &HookAbortError{
				EventType: EventTypePreCommit,
				RunID:     preRunID,
				Err:       err,
			}
		}

		var branchMetaRangeID MetaRangeID
		var parentGeneration int
		if branch.CommitID != "" {
			branchCommit, err := g.RefManager.GetCommit(ctx, repository, branch.CommitID)
			if err != nil {
				return nil, fmt.Errorf("get commit: %w", err)
			}
			branchMetaRangeID = branchCommit.MetaRangeID
			parentGeneration = branchCommit.Generation
		}
		commit.Generation = parentGeneration + 1
		if params.SourceMetaRange != nil {
			empty, err := g.isSealedEmpty(ctx, repository, branch)
			if err != nil {
				return nil, fmt.Errorf("checking empty sealed: %w", err)
			}
			if !empty {
				return nil, ErrCommitMetaRangeDirtyBranch
			}
			commit.MetaRangeID = *params.SourceMetaRange
		} else {
			changes, err := g.sealedTokensIterator(ctx, branch)
			if err != nil {
				return nil, err
			}
			defer changes.Close()
			// returns err if the commit is empty (no changes)
			commit.MetaRangeID, _, err = g.CommittedManager.Commit(ctx, storageNamespace, branchMetaRangeID, changes)
			if err != nil {
				return nil, fmt.Errorf("commit: %w", err)
			}
		}
		sealedToDrop = branch.SealedTokens

		// add commit
		newCommitID, err = g.RefManager.AddCommit(ctx, repository, commit)
		if err != nil {
			return nil, fmt.Errorf("add commit: %w", err)
		}

		branch.CommitID = newCommitID
		branch.SealedTokens = make([]StagingToken, 0)
		return branch, nil
	})
	if err != nil {
		return "", err
	}

	g.dropTokens(ctx, sealedToDrop...)

	postRunID := g.hooks.NewRunID()
	err = g.hooks.PostCommitHook(ctx, HookRecord{
		EventType:        EventTypePostCommit,
		RunID:            postRunID,
		RepositoryID:     repository.RepositoryID,
		StorageNamespace: storageNamespace,
		SourceRef:        newCommitID.Ref(),
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

func (g *KVGraveler) retryBranchUpdate(ctx context.Context, repository *RepositoryRecord, branchID BranchID, f BranchUpdateFunc) error {
	const (
		maxInterval = 5
		setTries    = 3
	)
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = maxInterval * time.Second

	try := 1
	err := backoff.Retry(func() error {
		// TODO(eden) issue 3586 - if the branch commit id hasn't changed, update the fields instead of fail
		err := g.RefManager.BranchUpdate(ctx, repository, branchID, f)
		if err != nil && !errors.Is(err, kv.ErrPredicateFailed) {
			return backoff.Permanent(err)
		}
		if err != nil && try < setTries {
			g.log.WithField("try", try).
				WithField("branchID", branchID).
				Info("Retrying update branch")
			try += 1
			return err
		}
		return nil
	}, bo)
	if try == setTries {
		return fmt.Errorf("update branch: %w", ErrTooManyTries)
	}
	return err
}

func validateCommitParent(ctx context.Context, repository *RepositoryRecord, commit Commit, manager RefManager) (CommitID, error) {
	if len(commit.Parents) > 1 {
		return "", ErrMultipleParents
	}
	if len(commit.Parents) == 0 {
		return "", nil
	}

	parentCommitID := commit.Parents[0]
	_, err := manager.GetCommit(ctx, repository, parentCommitID)
	if err != nil {
		return "", fmt.Errorf("get parent commit %s: %w", parentCommitID, err)
	}
	return parentCommitID, nil
}

func CommitExists(ctx context.Context, repository *RepositoryRecord, commitID CommitID, manager RefManager) (bool, error) {
	_, err := manager.GetCommit(ctx, repository, commitID)
	if err == nil {
		// commit already exists
		return true, nil
	}
	if !errors.Is(err, ErrCommitNotFound) {
		return false, fmt.Errorf("getting commit %s: %w", commitID, err)
	}
	return false, nil
}

func (g *KVGraveler) AddCommitToBranchHead(ctx context.Context, repository *RepositoryRecord, branchID BranchID, commit Commit) (CommitID, error) {
	// parentCommitID should always match the HEAD of the branch.
	// Empty parentCommitID matches first commit of the branch.
	parentCommitID, err := validateCommitParent(ctx, repository, commit, g.RefManager)
	if err != nil {
		return "", err
	}

	// verify access to meta range
	ok, err := g.CommittedManager.Exists(ctx, repository.StorageNamespace, commit.MetaRangeID)
	if err != nil {
		return "", fmt.Errorf("commit missing meta range %s: %w", commit.MetaRangeID, err)
	}
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrMetaRangeNotFound, commit.MetaRangeID)
	}

	// add commit to our ref manager
	commitID, err := g.RefManager.AddCommit(ctx, repository, commit)
	if err != nil {
		return "", fmt.Errorf("adding commit: %w", err)
	}

	if err := g.prepareForCommitIDUpdate(ctx, repository, branchID); err != nil {
		return "", err
	}

	var tokensToDrop []StagingToken
	// update branch with commit after verify:
	// 1. commit parent is the current branch head
	// 2. branch staging is empty
	err = g.RefManager.BranchUpdate(ctx, repository, branchID, func(branch *Branch) (*Branch, error) {
		if branch.CommitID != parentCommitID {
			return nil, ErrCommitNotHeadBranch
		}

		empty, err := g.isSealedEmpty(ctx, repository, branch)
		if err != nil {
			return nil, err
		}
		if !empty {
			return nil, ErrConflictFound
		}

		tokensToDrop = branch.SealedTokens
		return &Branch{
			CommitID:     commitID,
			StagingToken: branch.StagingToken,
			SealedTokens: []StagingToken{},
		}, nil
	})
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			err = ErrConflictFound
		}
		return "", err
	}

	g.dropTokens(ctx, tokensToDrop...)
	return commitID, nil
}

func (g *KVGraveler) AddCommit(ctx context.Context, repository *RepositoryRecord, commit Commit) (CommitID, error) {
	// at least a single parent must exists
	if len(commit.Parents) == 0 {
		return "", ErrAddCommitNoParent
	}
	_, err := validateCommitParent(ctx, repository, commit, g.RefManager)
	if err != nil {
		return "", err
	}

	// check if commit already exists.
	commitID := CommitID(ident.NewHexAddressProvider().ContentAddress(commit))
	if exists, err := CommitExists(ctx, repository, commitID, g.RefManager); err != nil {
		return "", err
	} else if exists {
		return commitID, nil
	}

	commitID, err = g.addCommitNoLock(ctx, repository, commit)
	if err != nil {
		return "", fmt.Errorf("adding commit: %w", err)
	}

	return commitID, nil
}

// addCommitNoLock lower API used to add commit into a repository. It will verify that the commit meta-range is accessible but will not lock any metadata update.
func (g *KVGraveler) addCommitNoLock(ctx context.Context, repository *RepositoryRecord, commit Commit) (CommitID, error) {
	// verify access to meta range
	ok, err := g.CommittedManager.Exists(ctx, repository.StorageNamespace, commit.MetaRangeID)
	if err != nil {
		return "", fmt.Errorf("checking for meta range %s: %w", commit.MetaRangeID, err)
	}
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrMetaRangeNotFound, commit.MetaRangeID)
	}

	// add commit
	commitID, err := g.RefManager.AddCommit(ctx, repository, commit)
	if err != nil {
		return "", fmt.Errorf("add commit: %w", err)
	}
	return commitID, nil
}

func (g *KVGraveler) isStagingEmpty(ctx context.Context, repository *RepositoryRecord, branch *Branch) (bool, error) {
	itr, err := g.listStagingArea(ctx, branch)
	if err != nil {
		return false, err
	}
	defer itr.Close()

	// Iterating over staging area (staging + sealed) of the branch and check for entries
	return g.checkEmpty(ctx, repository, branch, itr)
}

// checkEmpty - staging iterator is not considered empty IFF it contains any non-tombstone entry
// or a tombstone entry exists for a key which is already committed
func (g *KVGraveler) checkEmpty(ctx context.Context, repository *RepositoryRecord, branch *Branch, changesIt ValueIterator) (bool, error) {
	commit, err := g.RefManager.GetCommit(ctx, repository, branch.CommitID)
	if err != nil {
		return false, err
	}
	committedList, err := g.CommittedManager.List(ctx, repository.StorageNamespace, commit.MetaRangeID)
	if err != nil {
		return false, err
	}

	diffIt := NewUncommittedDiffIterator(ctx, committedList, changesIt)
	defer diffIt.Close()

	return !diffIt.Next(), nil
}

func (g *KVGraveler) isSealedEmpty(ctx context.Context, repository *RepositoryRecord, branch *Branch) (bool, error) {
	if len(branch.SealedTokens) == 0 {
		return true, nil
	}
	itrs, err := g.sealedTokensIterator(ctx, branch)
	if err != nil {
		return false, err
	}
	defer itrs.Close()
	return g.checkEmpty(ctx, repository, branch, itrs)
}

// dropTokens deletes all staging area entries of a given branch from store
func (g *KVGraveler) dropTokens(ctx context.Context, tokens ...StagingToken) {
	for _, token := range tokens {
		err := g.StagingManager.DropAsync(ctx, token)
		if err != nil {
			logging.Default().WithError(err).WithField("staging_token", token).Error("Failed to drop staging token")
		}
	}
}

func (g *KVGraveler) Reset(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error {
	isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
	if err != nil {
		return err
	}
	if isProtected {
		return ErrWriteToProtectedBranch
	}
	tokensToDrop := make([]StagingToken, 0)
	err = g.RefManager.BranchUpdate(ctx, repository, branchID, func(branch *Branch) (*Branch, error) {
		// Save current branch tokens for drop
		tokensToDrop = append(tokensToDrop, branch.StagingToken)
		tokensToDrop = append(tokensToDrop, branch.SealedTokens...)

		// Zero tokens and try to set branch
		branch.StagingToken = GenerateStagingToken(repository.RepositoryID, branchID)
		branch.SealedTokens = make([]StagingToken, 0)
		return branch, nil
	})
	if err != nil { // Branch update failed, don't drop staging tokens
		return err
	}

	g.dropTokens(ctx, tokensToDrop...)
	return nil
}

// resetKey resets given key on branch
// Since we cannot (will not) modify sealed tokens data, we overwrite changes done on entry on a new staging token, effectively reverting it
// to the current state in the branch committed data. If entry is not committed return an error
func (g *KVGraveler) resetKey(ctx context.Context, repository *RepositoryRecord, branch *Branch, key Key, stagedValue *Value, st StagingToken) error {
	isCommitted := true
	committed, err := g.Get(ctx, repository, branch.CommitID.Ref(), key)
	if err != nil {
		if !errors.Is(err, ErrNotFound) {
			return err
		}
		isCommitted = false
	}

	if isCommitted { // entry committed and changed in staging area => override with entry from commit
		if stagedValue != nil && bytes.Equal(committed.Identity, stagedValue.Identity) {
			return nil // No change
		}
		return g.StagingManager.Set(ctx, st, key, committed, true)
		// entry not committed and changed in staging area => override with tombstone
		// If not committed and staging == tombstone => ignore
	} else if !isCommitted && stagedValue != nil {
		return g.StagingManager.Set(ctx, st, key, nil, true)
	}

	return nil
}

func (g *KVGraveler) ResetKey(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error {
	isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
	if err != nil {
		return err
	}
	if isProtected {
		return ErrWriteToProtectedBranch
	}

	branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
	if err != nil {
		return fmt.Errorf("getting branch: %w", err)
	}

	staged, err := g.getFromStagingArea(ctx, branch, key)
	if err != nil {
		if errors.Is(err, ErrNotFound) { // If key is not in staging => nothing to do
			return nil
		}
		return err
	}

	err = g.resetKey(ctx, repository, branch, key, staged, branch.StagingToken)
	if err != nil {
		if !errors.Is(err, ErrNotFound) { // Not found in staging => ignore
			return err
		}
	}

	// The branch staging-token might have changed since we read it, and that's fine.
	// If a commit started, we may or may not include it in the commit.
	// We don't need to repeat the reset action for the new staging-token, since
	// every write to it started after the reset, hench it's ok to ignore it.
	return nil
}

func (g *KVGraveler) ResetPrefix(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error {
	isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
	if err != nil {
		return err
	}
	if isProtected {
		return ErrWriteToProtectedBranch
	}
	// New sealed tokens list after change includes current staging token
	newSealedTokens := make([]StagingToken, 0)
	newStagingToken := GenerateStagingToken(repository.RepositoryID, branchID)

	err = g.RefManager.BranchUpdate(ctx, repository, branchID, func(branch *Branch) (*Branch, error) {
		newSealedTokens = []StagingToken{branch.StagingToken}
		newSealedTokens = append(newSealedTokens, branch.SealedTokens...)

		// Reset keys by prefix on the new staging token
		itr, err := g.listStagingArea(ctx, branch)
		if err != nil {
			return nil, err
		}
		defer itr.Close()
		itr.SeekGE(key)
		var wg multierror.Group
		for itr.Next() {
			value := itr.Value()
			if !bytes.HasPrefix(value.Key, key) { // We passed the prefix - exit the loop
				break
			}
			wg.Go(func() error {
				err = g.resetKey(ctx, repository, branch, value.Key, value.Value, newStagingToken)
				if err != nil {
					return err
				}
				return nil
			})
		}
		err = wg.Wait().ErrorOrNil()
		if err != nil {
			return nil, err
		}

		// replace branch tokens with new staging/sealed tokens
		branch.StagingToken = newStagingToken
		branch.SealedTokens = newSealedTokens
		return branch, nil
	})
	if err != nil { // Cleanup of new staging token in case of error
		g.dropTokens(ctx, newStagingToken)
	}
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
func (g *KVGraveler) Revert(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref, parentNumber int, commitParams CommitParams) (CommitID, error) {
	commitRecord, err := g.dereferenceCommit(ctx, repository, ref)
	if err != nil {
		return "", fmt.Errorf("get commit from ref %s: %w", ref, err)
	}
	if len(commitRecord.Parents) > 1 && parentNumber <= 0 {
		// if commit has more than one parent, must explicitly specify parent number
		return "", ErrRevertMergeNoParent
	}
	if parentNumber > 0 {
		// validate parent is in range:
		if parentNumber > len(commitRecord.Parents) { // parent number is 1-based
			return "", fmt.Errorf("%w: parent %d", ErrRevertParentOutOfRange, parentNumber)
		}
		parentNumber--
	}

	if err := g.prepareForCommitIDUpdate(ctx, repository, branchID); err != nil {
		return "", err
	}

	var commitID CommitID
	var tokensToDrop []StagingToken
	err = g.RefManager.BranchUpdate(ctx, repository, branchID, func(branch *Branch) (*Branch, error) {
		if empty, err := g.isSealedEmpty(ctx, repository, branch); err != nil {
			return nil, err
		} else if !empty {
			return nil, fmt.Errorf("%s: %w", branchID, ErrDirtyBranch)
		}
		var parentMetaRangeID MetaRangeID
		if len(commitRecord.Parents) > 0 {
			parentCommit, err := g.dereferenceCommit(ctx, repository, commitRecord.Parents[parentNumber].Ref())
			if err != nil {
				return nil, fmt.Errorf("get commit from ref %s: %w", commitRecord.Parents[parentNumber], err)
			}
			parentMetaRangeID = parentCommit.MetaRangeID
		}
		branchCommit, err := g.dereferenceCommit(ctx, repository, branch.CommitID.Ref())
		if err != nil {
			return nil, fmt.Errorf("get commit from ref %s: %w", branch.CommitID, err)
		}
		// merge from the parent to the top of the branch, with the given ref as the merge base:
		metaRangeID, err := g.CommittedManager.Merge(ctx, repository.StorageNamespace, branchCommit.MetaRangeID,
			parentMetaRangeID, commitRecord.MetaRangeID, MergeStrategyNone)
		if err != nil {
			if !errors.Is(err, ErrUserVisible) {
				err = fmt.Errorf("merge: %w", err)
			}
			return nil, err
		}
		commit := NewCommit()
		commit.Committer = commitParams.Committer
		commit.Message = commitParams.Message
		commit.MetaRangeID = metaRangeID
		commit.Parents = []CommitID{branch.CommitID}
		commit.Metadata = commitParams.Metadata
		commit.Generation = branchCommit.Generation + 1
		commitID, err = g.RefManager.AddCommit(ctx, repository, commit)
		if err != nil {
			return nil, fmt.Errorf("add commit: %w", err)
		}

		tokensToDrop = branch.SealedTokens
		branch.SealedTokens = []StagingToken{}
		branch.CommitID = commitID
		return branch, nil
	})
	if err != nil {
		return "", fmt.Errorf("update branch: %w", err)
	}

	g.dropTokens(ctx, tokensToDrop...)
	return commitID, nil
}

func (g *KVGraveler) Merge(ctx context.Context, repository *RepositoryRecord, destination BranchID, source Ref, commitParams CommitParams, strategy string) (CommitID, error) {
	var (
		preRunID string
		commit   Commit
		commitID CommitID
	)

	storageNamespace := repository.StorageNamespace
	if err := g.prepareForCommitIDUpdate(ctx, repository, destination); err != nil {
		return "", err
	}

	var tokensToDrop []StagingToken
	// No retries on any failure during the merge. If the branch changed, it's either that commit is in progress, commit occurred,
	// or some other branch changing operation. If commit is in-progress, then staging area wasn't empty after we checked so not retrying is ok.
	// If another commit/merge succeeded, then the user should decide whether to retry the merge.
	err := g.RefManager.BranchUpdate(ctx, repository, destination, func(branch *Branch) (*Branch, error) {
		empty, err := g.isSealedEmpty(ctx, repository, branch)
		if err != nil {
			return nil, fmt.Errorf("check if staging empty: %w", err)
		}
		if !empty {
			return nil, fmt.Errorf("%s: %w", destination, ErrDirtyBranch)
		}
		fromCommit, toCommit, baseCommit, err := g.getCommitsForMerge(ctx, repository, source, Ref(destination))
		if err != nil {
			return nil, err
		}
		g.log.WithFields(logging.Fields{
			"repository":             source,
			"source":                 source,
			"destination":            destination,
			"source_meta_range":      fromCommit.MetaRangeID,
			"destination_meta_range": toCommit.MetaRangeID,
			"base_meta_range":        baseCommit.MetaRangeID,
		}).Trace("Merge")
		mergeStrategy := MergeStrategyNone
		if strategy == MergeStrategyDestWins {
			mergeStrategy = MergeStrategyDest
		}
		if strategy == MergeStrategySrcWins {
			mergeStrategy = MergeStrategySource
		}
		metaRangeID, err := g.CommittedManager.Merge(ctx, storageNamespace, toCommit.MetaRangeID, fromCommit.MetaRangeID, baseCommit.MetaRangeID, mergeStrategy)
		if err != nil {
			if !errors.Is(err, ErrUserVisible) {
				err = fmt.Errorf("merge in CommitManager: %w", err)
			}
			return nil, err
		}
		commit = NewCommit()
		commit.Committer = commitParams.Committer
		commit.Message = commitParams.Message
		commit.MetaRangeID = metaRangeID
		commit.Parents = []CommitID{toCommit.CommitID, fromCommit.CommitID}
		if toCommit.Generation > fromCommit.Generation {
			commit.Generation = toCommit.Generation + 1
		} else {
			commit.Generation = fromCommit.Generation + 1
		}
		commit.Metadata = commitParams.Metadata
		preRunID = g.hooks.NewRunID()
		err = g.hooks.PreMergeHook(ctx, HookRecord{
			EventType:        EventTypePreMerge,
			RunID:            preRunID,
			RepositoryID:     repository.RepositoryID,
			StorageNamespace: storageNamespace,
			BranchID:         destination,
			SourceRef:        fromCommit.CommitID.Ref(),
			Commit:           commit,
		})
		if err != nil {
			return nil, &HookAbortError{
				EventType: EventTypePreMerge,
				RunID:     preRunID,
				Err:       err,
			}
		}
		commitID, err = g.RefManager.AddCommit(ctx, repository, commit)
		if err != nil {
			return nil, fmt.Errorf("add commit: %w", err)
		}

		tokensToDrop = branch.SealedTokens
		branch.SealedTokens = []StagingToken{}
		branch.CommitID = commitID
		return branch, nil
	})
	if err != nil {
		return "", fmt.Errorf("update branch %s: %w", destination, err)
	}

	g.dropTokens(ctx, tokensToDrop...)
	postRunID := g.hooks.NewRunID()
	err = g.hooks.PostMergeHook(ctx, HookRecord{
		EventType:        EventTypePostMerge,
		RunID:            postRunID,
		RepositoryID:     repository.RepositoryID,
		StorageNamespace: storageNamespace,
		BranchID:         destination,
		SourceRef:        commitID.Ref(),
		Commit:           commit,
		CommitID:         commitID,
		PreRunID:         preRunID,
	})
	if err != nil {
		g.log.
			WithError(err).
			WithField("run_id", postRunID).
			WithField("pre_run_id", preRunID).
			Error("Post-merge hook failed")
	}
	return commitID, nil
}

// DiffUncommitted returns DiffIterator between committed data and staging area of a branch
func (g *KVGraveler) DiffUncommitted(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (DiffIterator, error) {
	branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
	if err != nil {
		return nil, err
	}
	var metaRangeID MetaRangeID
	if branch.CommitID != "" {
		commit, err := g.RefManager.GetCommit(ctx, repository, branch.CommitID)
		if err != nil {
			return nil, err
		}
		metaRangeID = commit.MetaRangeID
	}

	valueIterator, err := g.listStagingArea(ctx, branch)
	if err != nil {
		return nil, err
	}
	var committedValueIterator ValueIterator
	if metaRangeID != "" {
		committedValueIterator, err = g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
		if err != nil {
			valueIterator.Close()
			return nil, err
		}
	}
	return NewUncommittedDiffIterator(ctx, committedValueIterator, valueIterator), nil
}

// dereferenceCommit will dereference and load the commit record based on 'ref'.
//   will return an error if 'ref' points to an explicit staging area
func (g *KVGraveler) dereferenceCommit(ctx context.Context, repository *RepositoryRecord, ref Ref) (*CommitRecord, error) {
	reference, err := g.Dereference(ctx, repository, ref)
	if err != nil {
		return nil, err
	}
	if reference.ResolvedBranchModifier == ResolvedBranchModifierStaging {
		return nil, fmt.Errorf("reference '%s': %w", ref, ErrDereferenceCommitWithStaging)
	}
	commit, err := g.RefManager.GetCommit(ctx, repository, reference.CommitID)
	if err != nil {
		return nil, err
	}
	return &CommitRecord{
		CommitID: reference.CommitID,
		Commit:   commit,
	}, nil
}

func (g *KVGraveler) Diff(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error) {
	leftCommit, err := g.dereferenceCommit(ctx, repository, left)
	if err != nil {
		return nil, err
	}
	rightRawRef, err := g.Dereference(ctx, repository, right)
	if err != nil {
		return nil, err
	}
	rightCommit, err := g.RefManager.GetCommit(ctx, repository, rightRawRef.CommitID)
	if err != nil {
		return nil, err
	}
	diff, err := g.CommittedManager.Diff(ctx, repository.StorageNamespace, leftCommit.MetaRangeID, rightCommit.MetaRangeID)
	if err != nil {
		return nil, err
	}
	if rightRawRef.ResolvedBranchModifier != ResolvedBranchModifierStaging {
		return diff, nil
	}
	leftValueIterator, err := g.CommittedManager.List(ctx, repository.StorageNamespace, leftCommit.MetaRangeID)
	if err != nil {
		return nil, err
	}

	rightBranch, err := g.RefManager.GetBranch(ctx, repository, rightRawRef.BranchID)
	if err != nil {
		leftValueIterator.Close()
		return nil, err
	}
	stagingIterator, err := g.listStagingArea(ctx, rightBranch)
	if err != nil {
		leftValueIterator.Close()
		return nil, err
	}
	return NewCombinedDiffIterator(diff, leftValueIterator, stagingIterator), nil
}

func (g *KVGraveler) getCommitsForMerge(ctx context.Context, repository *RepositoryRecord, from Ref, to Ref) (*CommitRecord, *CommitRecord, *Commit, error) {
	fromCommit, err := g.dereferenceCommit(ctx, repository, from)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get commit by ref %s: %w", from, err)
	}
	toCommit, err := g.dereferenceCommit(ctx, repository, to)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get commit by branch %s: %w", to, err)
	}
	baseCommit, err := g.RefManager.FindMergeBase(ctx, repository, fromCommit.CommitID, toCommit.CommitID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("find merge base: %w", err)
	}
	if baseCommit == nil {
		return nil, nil, nil, ErrNoMergeBase
	}
	return fromCommit, toCommit, baseCommit, nil
}

func (g *KVGraveler) Compare(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error) {
	fromCommit, toCommit, baseCommit, err := g.getCommitsForMerge(ctx, repository, right, left)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Compare(ctx, repository.StorageNamespace, toCommit.MetaRangeID, fromCommit.MetaRangeID, baseCommit.MetaRangeID)
}

func (g *KVGraveler) SetHooksHandler(handler HooksHandler) {
	if handler == nil {
		g.hooks = &HooksNoOp{}
	} else {
		g.hooks = handler
	}
}

func (g *KVGraveler) LoadCommits(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error {
	iter, err := g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
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
		if commit.GetGeneration() == 0 {
			return fmt.Errorf("dumps created by lakeFS versions before v0.61.0 are no longer supported: %w", ErrNoCommitGeneration)
		}
		commitID, err := g.RefManager.AddCommit(ctx, repository, Commit{
			Version:      CommitVersion(commit.Version),
			Committer:    commit.GetCommitter(),
			Message:      commit.GetMessage(),
			MetaRangeID:  MetaRangeID(commit.GetMetaRangeId()),
			CreationDate: commit.GetCreationDate().AsTime(),
			Parents:      parents,
			Metadata:     commit.GetMetadata(),
			Generation:   int(commit.GetGeneration()),
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

func (g *KVGraveler) LoadBranches(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error {
	iter, err := g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Next() {
		rawValue := iter.Value()
		branch := &BranchData{}
		err = proto.Unmarshal(rawValue.Data, branch)
		if err != nil {
			return err
		}
		branchID := BranchID(branch.Id)
		err = g.RefManager.SetBranch(ctx, repository, branchID, Branch{
			CommitID:     CommitID(branch.CommitId),
			StagingToken: GenerateStagingToken(repository.RepositoryID, branchID),
			SealedTokens: make([]StagingToken, 0),
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

func (g *KVGraveler) LoadTags(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error {
	iter, err := g.CommittedManager.List(ctx, repository.StorageNamespace, metaRangeID)
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
		err = g.RefManager.CreateTag(ctx, repository, tagID, CommitID(tag.CommitId))
		if err != nil {
			return err
		}
	}
	return iter.Err()
}

func (g *KVGraveler) GetMetaRange(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) (MetaRangeAddress, error) {
	return g.CommittedManager.GetMetaRange(ctx, repository.StorageNamespace, metaRangeID)
}

func (g *KVGraveler) GetRange(ctx context.Context, repository *RepositoryRecord, rangeID RangeID) (RangeAddress, error) {
	return g.CommittedManager.GetRange(ctx, repository.StorageNamespace, rangeID)
}

func (g *KVGraveler) DumpCommits(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error) {
	iter, err := g.RefManager.ListCommits(ctx, repository)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	schema, err := serializeSchemaDefinition(&CommitData{})
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRangeByIterator(ctx, repository.StorageNamespace,
		commitsToValueIterator(iter),
		Metadata{
			EntityTypeKey:             EntityTypeCommit,
			EntitySchemaKey:           EntitySchemaCommit,
			EntitySchemaDefinitionKey: schema,
		},
	)
}

func (g *KVGraveler) DumpBranches(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error) {
	iter, err := g.RefManager.ListBranches(ctx, repository)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	schema, err := serializeSchemaDefinition(&BranchData{})
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRangeByIterator(ctx, repository.StorageNamespace,
		branchesToValueIterator(iter),
		Metadata{
			EntityTypeKey:             EntityTypeBranch,
			EntitySchemaKey:           EntitySchemaBranch,
			EntitySchemaDefinitionKey: schema,
		},
	)
}

func (g *KVGraveler) DumpTags(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error) {
	iter, err := g.RefManager.ListTags(ctx, repository)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	schema, err := serializeSchemaDefinition(&TagData{})
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.WriteMetaRangeByIterator(ctx, repository.StorageNamespace,
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

	SaveGarbageCollectionCommits(ctx context.Context, repository *RepositoryRecord, rules *GarbageCollectionRules, previouslyExpiredCommits []CommitID) (string, error)
	GetRunExpiredCommits(ctx context.Context, storageNamespace StorageNamespace, runID string) ([]CommitID, error)
	GetCommitsCSVLocation(runID string, sn StorageNamespace) (string, error)
	GetAddressesLocation(sn StorageNamespace) (string, error)
}

type ProtectedBranchesManager interface {
	// Add creates a rule for the given name pattern, blocking the given actions.
	// Returns ErrRuleAlreadyExists if there is already a rule for the given pattern.
	Add(ctx context.Context, repository *RepositoryRecord, branchNamePattern string, blockedActions []BranchProtectionBlockedAction) error
	// Delete deletes the rule for the given name pattern, or returns ErrRuleNotExists if there is no such rule.
	Delete(ctx context.Context, repository *RepositoryRecord, branchNamePattern string) error
	// Get returns the list of blocked actions for the given name pattern, or nil if no rule was defined for the pattern.
	Get(ctx context.Context, repository *RepositoryRecord, branchNamePattern string) ([]BranchProtectionBlockedAction, error)
	// GetRules returns all branch protection rules for the repository
	GetRules(ctx context.Context, repository *RepositoryRecord) (*BranchProtectionRules, error)
	// IsBlocked returns whether the action is blocked by any branch protection rule matching the given branch.
	IsBlocked(ctx context.Context, repository *RepositoryRecord, branchID BranchID, action BranchProtectionBlockedAction) (bool, error)
}

// NewRepoInstanceID Returns a new unique identifier for the repository instance
func NewRepoInstanceID() string {
	tm := time.Now().UTC()
	return xid.NewWithTime(tm).String()
}
