package graveler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
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

//go:generate go run github.com/golang/mock/mockgen@v1.6.0 -source=graveler.go -destination=mock/graveler.go -package=mock

const (
	BranchUpdateMaxInterval = 5 * time.Second
	BranchUpdateMaxTries    = 10

	DeleteKeysMaxSize = 1000

	// BranchWriteMaxTries is the number of times to repeat the set operation if the staging token changed
	BranchWriteMaxTries = 3

	RepoMetadataUpdateMaxInterval    = 5 * time.Second
	RepoMetadataUpdateMaxElapsedTime = 15 * time.Second
	RepoMetadataUpdateRandomFactor   = 0.5
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
//
//	ordered modifiers that applied to the reference.
//
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
//
//	Type: Branch / Tag / Commit
//	BranchID: for type ReferenceTypeBranch will hold the branch ID
//	ResolvedBranchModifier: branch indicator if resolved to a branch the latest commit, staging or none was specified.
//	CommitID: the commit ID of the branch head,  tag or specific hash.
//	StagingToken: empty if ResolvedBranchModifier is ResolvedBranchModifierCommitted.
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
	MergeStrategySrc

	MergeStrategyNoneStr     = "default"
	MergeStrategyDestWinsStr = "dest-wins"
	MergeStrategySrcWinsStr  = "source-wins"

	MergeStrategyMetadataKey = ".lakefs.merge.strategy"
)

// mergeStrategyString String representation for MergeStrategy consts. Pay attention to the order!
var mergeStrategyString = []string{
	MergeStrategyNoneStr,
	MergeStrategyDestWinsStr,
	MergeStrategySrcWinsStr,
}

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

// GetOptions controls get request defaults
type GetOptions struct {
	// StageOnly fetch key from stage area only. Default (false) will lookup stage and committed data.
	StageOnly bool
}

type GetOptionsFunc func(opts *GetOptions)

func WithStageOnly(v bool) GetOptionsFunc {
	return func(opts *GetOptions) {
		opts.StageOnly = v
	}
}

type SetOptions struct {
	IfAbsent bool
	// MaxTries set number of times we try to perform the operation before we fail with BranchWriteMaxTries.
	// By default, 0 - we try BranchWriteMaxTries
	MaxTries int
}

type SetOptionsFunc func(opts *SetOptions)

func WithIfAbsent(v bool) SetOptionsFunc {
	return func(opts *SetOptions) {
		opts.IfAbsent = v
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

// ImportID represents an import process id in the ref-store
type ImportID string

type ImportStatus struct {
	ID          ImportID
	Completed   bool
	UpdatedAt   time.Time
	Progress    int64
	MetaRangeID MetaRangeID
	Commit      *CommitRecord
	Error       error
}

// StagingToken represents a namespace for writes to apply as uncommitted
type StagingToken string

// Metadata key/value strings to hold metadata information on value and commit
type Metadata map[string]string

// Repository represents repository metadata
type Repository struct {
	StorageNamespace StorageNamespace
	CreationDate     time.Time
	DefaultBranchID  BranchID
	// RepositoryState represents the state of the repository, only ACTIVE repository is considered a valid one.
	// other states represent in invalid temporary or terminal state
	State RepositoryState
	// InstanceUID identifies repository in a unique way. Since repositories with same name can be deleted and recreated
	// this field identifies the specific instance, and used in the KV store key path to store all the entities belonging
	// to this specific instantiation of repo with the given ID
	InstanceUID string
}

type RepositoryMetadata map[string]string

const MetadataKeyLastImportTimeStamp = ".lakefs.last.import.timestamp"

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
	Version      CommitVersion
	Committer    string
	Message      string
	MetaRangeID  MetaRangeID
	CreationDate time.Time
	Parents      CommitParents
	Metadata     Metadata
	Generation   int
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

type safeBranchWriteOptions struct {
	// MaxTries number of tries to perform operation while branch changes. Default: BranchWriteMaxTries
	MaxTries int
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

type GarbageCollectionRunMetadata struct {
	RunID string
	// Location of expired commits CSV file on object store
	CommitsCSVLocation string
	// Location of where to write expired addresses on object store
	AddressLocation string
}

type RepoMetadataUpdateFunc func(metadata RepositoryMetadata) (RepositoryMetadata, error)

type KeyValueStore interface {
	// Get returns value from repository / reference by key, nil value is a valid value for tombstone
	// returns error if value does not exist
	Get(ctx context.Context, repository *RepositoryRecord, ref Ref, key Key, opts ...GetOptionsFunc) (*Value, error)

	// GetByCommitID returns value from repository / commit by key and error if value does not exist
	GetByCommitID(ctx context.Context, repository *RepositoryRecord, commitID CommitID, key Key) (*Value, error)

	// GetRangeIDByKey returns rangeID from the commitID that contains the key
	GetRangeIDByKey(ctx context.Context, repository *RepositoryRecord, commitID CommitID, key Key) (RangeID, error)

	// Set stores value on repository / branch by key. nil value is a valid value for tombstone
	Set(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key, value Value, opts ...SetOptionsFunc) error

	// Delete value from repository / branch by key
	Delete(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error

	// DeleteBatch delete values from repository / branch by batch of keys
	DeleteBatch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, keys []Key) error

	// List lists values on repository / ref
	List(ctx context.Context, repository *RepositoryRecord, ref Ref, batchSize int) (ValueIterator, error)

	// ListStaging returns ValueIterator for branch staging area. Exposed to be used by catalog in PrepareGCUncommitted
	ListStaging(ctx context.Context, branch *Branch, batchSize int) (ValueIterator, error)
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

	// GetRepositoryMetadata returns repository user metadata
	GetRepositoryMetadata(ctx context.Context, repositoryID RepositoryID) (RepositoryMetadata, error)

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
	Log(ctx context.Context, repository *RepositoryRecord, commitID CommitID, firstParent bool) (CommitIterator, error)

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

	// CherryPick creates a patch to the commit given as 'ref', and applies it as a new commit on the given branch.
	CherryPick(ctx context.Context, repository *RepositoryRecord, id BranchID, reference Ref, number *int, committer string) (CommitID, error)

	// Merge merges 'source' into 'destination' and returns the commit id for the created merge commit.
	Merge(ctx context.Context, repository *RepositoryRecord, destination BranchID, source Ref, commitParams CommitParams, strategy string) (CommitID, error)

	// Import Creates a merge-commit using source MetaRangeID into destination branch with a src-wins merge strategy
	Import(ctx context.Context, repository *RepositoryRecord, destination BranchID, source MetaRangeID, commitParams CommitParams) (CommitID, error)

	// DiffUncommitted returns iterator to scan the changes made on the branch
	DiffUncommitted(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (DiffIterator, error)

	// Diff returns the changes between 'left' and 'right' ref.
	// This is similar to a two-dot (left..right) diff in git.
	Diff(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error)

	// Compare returns the difference between the commit where 'left' was last synced into 'right', and the most recent commit of `right`.
	// This is similar to a three-dot (from...to) diff in git.
	Compare(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error)

	// FindMergeBase returns the 'from' commit, the 'to' commit and the merge base commit of 'from' and 'to' commits.
	FindMergeBase(ctx context.Context, repository *RepositoryRecord, from Ref, to Ref) (*CommitRecord, *CommitRecord, *Commit, error)

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

	// GCGetUncommittedLocation returns full uri of the storage location of saved uncommitted files per runID
	GCGetUncommittedLocation(repository *RepositoryRecord, runID string) (string, error)

	GCNewRunID() string

	// GetBranchProtectionRules return all branch protection rules for the repository
	GetBranchProtectionRules(ctx context.Context, repository *RepositoryRecord) (*BranchProtectionRules, error)

	// DeleteBranchProtectionRule deletes the branch protection rule for the given pattern,
	// or return ErrRuleNotExists if no such rule exists.
	DeleteBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string) error

	// CreateBranchProtectionRule creates a rule for the given name pattern,
	// or returns ErrRuleAlreadyExists if there is already a rule for the pattern.
	CreateBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string, blockedActions []BranchProtectionBlockedAction) error

	// SetLinkAddress stores the address token under the repository. The token will be valid for addressTokenTime.
	// or return ErrAddressTokenAlreadyExists if a token already exists.
	SetLinkAddress(ctx context.Context, repository *RepositoryRecord, token string) error

	// VerifyLinkAddress returns nil if the token is valid (exists and not expired) and deletes it
	VerifyLinkAddress(ctx context.Context, repository *RepositoryRecord, token string) error

	// ListLinkAddresses lists address tokens on a repository
	ListLinkAddresses(ctx context.Context, repository *RepositoryRecord) (AddressTokenIterator, error)

	// DeleteExpiredLinkAddresses deletes expired tokens on a repository
	DeleteExpiredLinkAddresses(ctx context.Context, repository *RepositoryRecord) error

	// IsLinkAddressExpired returns nil if the token is valid and not expired
	IsLinkAddressExpired(token *LinkAddressData) (bool, error)

	// DeleteExpiredImports deletes expired imports on a given repository
	DeleteExpiredImports(ctx context.Context, repository *RepositoryRecord) error
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
	// Returns the created range info and in addition a list of records which were skipped due to out of order listing
	// which might happen in Azure ADLS Gen2 listing
	WriteRange(ctx context.Context, repository *RepositoryRecord, it ValueIterator) (*RangeInfo, error)
	// WriteMetaRange creates a new MetaRange from the given Ranges.
	WriteMetaRange(ctx context.Context, repository *RepositoryRecord, ranges []*RangeInfo) (*MetaRangeInfo, error)
	// StageObject stages given object to stagingToken.
	StageObject(ctx context.Context, stagingToken string, object ValueRecord) error
	// UpdateBranchToken updates the given branch stagingToken
	UpdateBranchToken(ctx context.Context, repository *RepositoryRecord, branchID, stagingToken string) error
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

type AddressTokenIterator interface {
	Next() bool
	SeekGE(address string)
	Value() *LinkAddressData
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

	// GetRepositoryMetadata gets repository user metadata
	GetRepositoryMetadata(ctx context.Context, repositoryID RepositoryID) (RepositoryMetadata, error)

	// SetRepositoryMetadata updates repository user metadata using the updateFunc
	SetRepositoryMetadata(ctx context.Context, repository *RepositoryRecord, updateFunc RepoMetadataUpdateFunc) error

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
	Log(ctx context.Context, repository *RepositoryRecord, commitID CommitID, firstParent bool) (CommitIterator, error)

	// ListCommits returns an iterator over all known commits, ordered by their commit ID
	ListCommits(ctx context.Context, repository *RepositoryRecord) (CommitIterator, error)

	// GCCommitIterator TODO (niro): Remove when DB implementation is deleted
	// GCCommitIterator temporary WA to support both DB and KV GC CommitIterator
	GCCommitIterator(ctx context.Context, repository *RepositoryRecord) (CommitIterator, error)

	// VerifyLinkAddress verifies the given address token
	VerifyLinkAddress(ctx context.Context, repository *RepositoryRecord, token string) error

	// SetLinkAddress creates address token
	SetLinkAddress(ctx context.Context, repository *RepositoryRecord, token string) error

	// ListLinkAddresses lists address tokens on a repository
	ListLinkAddresses(ctx context.Context, repository *RepositoryRecord) (AddressTokenIterator, error)

	// DeleteExpiredLinkAddresses deletes expired tokens on a repository
	DeleteExpiredLinkAddresses(ctx context.Context, repository *RepositoryRecord) error

	// IsLinkAddressExpired returns nil if the token is valid and not expired
	IsLinkAddressExpired(token *LinkAddressData) (bool, error)

	// DeleteExpiredImports deletes expired imports on a given repository
	DeleteExpiredImports(ctx context.Context, repository *RepositoryRecord) error
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

	// GetRangeIDByKey returns the RangeID that contains the given key.
	GetRangeIDByKey(ctx context.Context, ns StorageNamespace, id MetaRangeID, key Key) (RangeID, error)
}

// StagingManager manages entries in a staging area, denoted by a staging token
type StagingManager interface {
	// Get returns the value for the provided staging token and key
	// Returns ErrNotFound if no value found on key.
	Get(ctx context.Context, st StagingToken, key Key) (*Value, error)

	// Set writes a (possibly nil) value under the given staging token and key.
	// If requireExists is true - update key only if key already exists in store
	Set(ctx context.Context, st StagingToken, key Key, value *Value, requireExists bool) error

	// Update updates a (possibly nil) value under the given staging token and key.
	// Skip update in case 'ErrSkipUpdateValue' is returned from 'updateFunc'.
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

func (id ImportID) String() string {
	return string(id)
}

type Graveler struct {
	hooks                    HooksHandler
	CommittedManager         CommittedManager
	RefManager               RefManager
	StagingManager           StagingManager
	protectedBranchesManager ProtectedBranchesManager
	garbageCollectionManager GarbageCollectionManager
	// logger *without context* to be used for logging.  It should be
	// avoided in favour of g.log(ctx) in any operation where context is
	// available.
	logger logging.Logger
}

func NewGraveler(committedManager CommittedManager, stagingManager StagingManager, refManager RefManager, gcManager GarbageCollectionManager, protectedBranchesManager ProtectedBranchesManager) *Graveler {
	return &Graveler{
		hooks:                    &HooksNoOp{},
		CommittedManager:         committedManager,
		RefManager:               refManager,
		StagingManager:           stagingManager,
		protectedBranchesManager: protectedBranchesManager,
		garbageCollectionManager: gcManager,
		logger:                   logging.Default().WithField("service_name", "graveler_graveler"),
	}
}

func (g *Graveler) log(ctx context.Context) logging.Logger {
	return g.logger.WithContext(ctx)
}

func (g *Graveler) GetRepository(ctx context.Context, repositoryID RepositoryID) (*RepositoryRecord, error) {
	return g.RefManager.GetRepository(ctx, repositoryID)
}

func (g *Graveler) CreateRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, branchID BranchID) (*RepositoryRecord, error) {
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

func (g *Graveler) CreateBareRepository(ctx context.Context, repositoryID RepositoryID, storageNamespace StorageNamespace, defaultBranchID BranchID) (*RepositoryRecord, error) {
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

func (g *Graveler) ListRepositories(ctx context.Context) (RepositoryIterator, error) {
	return g.RefManager.ListRepositories(ctx)
}

func (g *Graveler) DeleteRepository(ctx context.Context, repositoryID RepositoryID) error {
	return g.RefManager.DeleteRepository(ctx, repositoryID)
}

func (g *Graveler) GetRepositoryMetadata(ctx context.Context, repositoryID RepositoryID) (RepositoryMetadata, error) {
	return g.RefManager.GetRepositoryMetadata(ctx, repositoryID)
}

func (g *Graveler) WriteRange(ctx context.Context, repository *RepositoryRecord, it ValueIterator) (*RangeInfo, error) {
	return g.CommittedManager.WriteRange(ctx, repository.StorageNamespace, it)
}

func (g *Graveler) WriteMetaRange(ctx context.Context, repository *RepositoryRecord, ranges []*RangeInfo) (*MetaRangeInfo, error) {
	return g.CommittedManager.WriteMetaRange(ctx, repository.StorageNamespace, ranges)
}

func (g *Graveler) StageObject(ctx context.Context, stagingToken string, object ValueRecord) error {
	return g.StagingManager.Set(ctx, StagingToken(stagingToken), object.Key, object.Value, false)
}

func (g *Graveler) UpdateBranchToken(ctx context.Context, repository *RepositoryRecord, branchID, stagingToken string) error {
	err := g.RefManager.BranchUpdate(ctx, repository, BranchID(branchID), func(branch *Branch) (*Branch, error) {
		isEmpty, err := g.isStagingEmpty(ctx, repository, branch)
		if err != nil {
			return nil, err
		}
		if !isEmpty {
			return nil, fmt.Errorf("branch staging is not empty: %w", ErrDirtyBranch)
		}
		tokensToDrop := []StagingToken{branch.StagingToken}
		tokensToDrop = append(tokensToDrop, branch.SealedTokens...)
		g.dropTokens(ctx, tokensToDrop...)
		branch.StagingToken = StagingToken(stagingToken)
		branch.SealedTokens = make([]StagingToken, 0)
		return branch, nil
	})
	return err
}

func (g *Graveler) WriteMetaRangeByIterator(ctx context.Context, repository *RepositoryRecord, it ValueIterator) (*MetaRangeID, error) {
	return g.CommittedManager.WriteMetaRangeByIterator(ctx, repository.StorageNamespace, it, nil)
}

func (g *Graveler) GetCommit(ctx context.Context, repository *RepositoryRecord, commitID CommitID) (*Commit, error) {
	return g.RefManager.GetCommit(ctx, repository, commitID)
}

func GenerateStagingToken(repositoryID RepositoryID, branchID BranchID) StagingToken {
	uid := uuid.New().String()
	return StagingToken(fmt.Sprintf("%s-%s:%s", repositoryID, branchID, uid))
}

func (g *Graveler) CreateBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref) (*Branch, error) {
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

func (g *Graveler) UpdateBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref) (*Branch, error) {
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
			return nil, ErrDirtyBranch
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
func (g *Graveler) prepareForCommitIDUpdate(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error {
	return g.retryBranchUpdate(ctx, repository, branchID, func(currBranch *Branch) (*Branch, error) {
		empty, err := g.isStagingEmpty(ctx, repository, currBranch)
		if err != nil {
			return nil, err
		}
		if !empty {
			return nil, ErrDirtyBranch
		}

		currBranch.SealedTokens = append([]StagingToken{currBranch.StagingToken}, currBranch.SealedTokens...)
		currBranch.StagingToken = GenerateStagingToken(repository.RepositoryID, branchID)
		return currBranch, nil
	})
}

func (g *Graveler) GetBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (*Branch, error) {
	return g.RefManager.GetBranch(ctx, repository, branchID)
}

func (g *Graveler) GetTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) (*CommitID, error) {
	return g.RefManager.GetTag(ctx, repository, tagID)
}

func (g *Graveler) CreateTag(ctx context.Context, repository *RepositoryRecord, tagID TagID, commitID CommitID) error {
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

func (g *Graveler) DeleteTag(ctx context.Context, repository *RepositoryRecord, tagID TagID) error {
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

func (g *Graveler) ListTags(ctx context.Context, repository *RepositoryRecord) (TagIterator, error) {
	return g.RefManager.ListTags(ctx, repository)
}

func (g *Graveler) Dereference(ctx context.Context, repository *RepositoryRecord, ref Ref) (*ResolvedRef, error) {
	rawRef, err := g.ParseRef(ref)
	if err != nil {
		return nil, err
	}
	return g.ResolveRawRef(ctx, repository, rawRef)
}

func (g *Graveler) ParseRef(ref Ref) (RawRef, error) {
	return g.RefManager.ParseRef(ref)
}

func (g *Graveler) ResolveRawRef(ctx context.Context, repository *RepositoryRecord, rawRef RawRef) (*ResolvedRef, error) {
	return g.RefManager.ResolveRawRef(ctx, repository, rawRef)
}

func (g *Graveler) Log(ctx context.Context, repository *RepositoryRecord, commitID CommitID, firstParent bool) (CommitIterator, error) {
	return g.RefManager.Log(ctx, repository, commitID, firstParent)
}

func (g *Graveler) ListBranches(ctx context.Context, repository *RepositoryRecord) (BranchIterator, error) {
	return g.RefManager.ListBranches(ctx, repository)
}

func (g *Graveler) DeleteBranch(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error {
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

func (g *Graveler) GetStagingToken(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (*StagingToken, error) {
	branch, err := g.RefManager.GetBranch(ctx, repository, branchID)
	if err != nil {
		return nil, err
	}
	return &branch.StagingToken, nil
}

func (g *Graveler) getGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord) (*GarbageCollectionRules, error) {
	return g.garbageCollectionManager.GetRules(ctx, repository.StorageNamespace)
}

func (g *Graveler) GetGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord) (*GarbageCollectionRules, error) {
	return g.getGarbageCollectionRules(ctx, repository)
}

func (g *Graveler) SetGarbageCollectionRules(ctx context.Context, repository *RepositoryRecord, rules *GarbageCollectionRules) error {
	return g.garbageCollectionManager.SaveRules(ctx, repository.StorageNamespace, rules)
}

func (g *Graveler) SaveGarbageCollectionCommits(ctx context.Context, repository *RepositoryRecord, previousRunID string) (*GarbageCollectionRunMetadata, error) {
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
		RunID:              runID,
		CommitsCSVLocation: commitsLocation,
		AddressLocation:    addressLocation,
	}, err
}

func (g *Graveler) GCGetUncommittedLocation(repository *RepositoryRecord, runID string) (string, error) {
	return g.garbageCollectionManager.GetUncommittedLocation(runID, repository.StorageNamespace)
}

func (g *Graveler) GCNewRunID() string {
	return g.garbageCollectionManager.NewID()
}

func (g *Graveler) GetBranchProtectionRules(ctx context.Context, repository *RepositoryRecord) (*BranchProtectionRules, error) {
	return g.protectedBranchesManager.GetRules(ctx, repository)
}

func (g *Graveler) DeleteBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string) error {
	return g.protectedBranchesManager.Delete(ctx, repository, pattern)
}

func (g *Graveler) CreateBranchProtectionRule(ctx context.Context, repository *RepositoryRecord, pattern string, blockedActions []BranchProtectionBlockedAction) error {
	return g.protectedBranchesManager.Add(ctx, repository, pattern, blockedActions)
}

// getFromStagingArea returns the most updated value of a given key in a branch staging area.
// Iterate over all tokens - staging + sealed in order of last modified. First appearance of key represents the latest update
// TODO: in most cases it is used by Get flow, assuming that usually the key will be found in committed we need to parallelize the get from tokens
func (g *Graveler) getFromStagingArea(ctx context.Context, b *Branch, key Key) (*Value, error) {
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

func (g *Graveler) Get(ctx context.Context, repository *RepositoryRecord, ref Ref, key Key, opts ...GetOptionsFunc) (*Value, error) {
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

	var options GetOptions
	for _, opt := range opts {
		opt(&options)
	}
	if options.StageOnly {
		return nil, ErrNotFound
	}

	// If key is not found in staging area (or reference is not a branch), return the key from committed
	commitID := reference.CommitID
	commit, err := g.RefManager.GetCommit(ctx, repository, commitID)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Get(ctx, repository.StorageNamespace, commit.MetaRangeID, key)
}

func (g *Graveler) GetByCommitID(ctx context.Context, repository *RepositoryRecord, commitID CommitID, key Key) (*Value, error) {
	// If key is not found in staging area (or reference is not a branch), return the key from committed
	commit, err := g.RefManager.GetCommit(ctx, repository, commitID)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Get(ctx, repository.StorageNamespace, commit.MetaRangeID, key)
}

func (g *Graveler) GetRangeIDByKey(ctx context.Context, repository *RepositoryRecord, commitID CommitID, key Key) (RangeID, error) {
	commit, err := g.RefManager.GetCommit(ctx, repository, commitID)
	if err != nil {
		return "", err
	}
	return g.CommittedManager.GetRangeIDByKey(ctx, repository.StorageNamespace, commit.MetaRangeID, key)
}

func (g *Graveler) Set(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key, value Value, opts ...SetOptionsFunc) error {
	isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
	if err != nil {
		return err
	}
	if isProtected {
		return ErrWriteToProtectedBranch
	}

	options := &SetOptions{}
	for _, opt := range opts {
		opt(options)
	}

	log := g.log(ctx).WithFields(logging.Fields{"key": key, "operation": "set"})
	return g.safeBranchWrite(ctx, log, repository, branchID, safeBranchWriteOptions{MaxTries: options.MaxTries}, func(branch *Branch) error {
		if !options.IfAbsent {
			return g.StagingManager.Set(ctx, branch.StagingToken, key, &value, false)
		}

		// verify the key not found
		_, err := g.Get(ctx, repository, Ref(branchID), key)
		if err == nil || !errors.Is(err, ErrNotFound) {
			return err
		}

		// update stage with new value only if key not found or tombstone
		return g.StagingManager.Update(ctx, branch.StagingToken, key, func(currentValue *Value) (*Value, error) {
			if currentValue == nil || currentValue.Identity == nil {
				return &value, nil
			}
			return nil, ErrSkipValueUpdate
		})
	})
}

// safeBranchWrite is a helper function that wraps a branch write operation with validation that the staging token
// didn't change while writing to the branch.
func (g *Graveler) safeBranchWrite(ctx context.Context, log logging.Logger, repository *RepositoryRecord, branchID BranchID,
	options safeBranchWriteOptions, stagingOperation func(branch *Branch) error,
) error {
	if options.MaxTries == 0 {
		options.MaxTries = BranchWriteMaxTries
	}
	var try int
	for try = 0; try < options.MaxTries; try++ {
		branchPreOp, err := g.GetBranch(ctx, repository, branchID)
		if err != nil {
			return err
		}
		if err = stagingOperation(branchPreOp); err != nil {
			return err
		}

		// Checking if the token has changed.
		// If it changed, we need to write the changes to the branch's new staging token
		branchPostOp, err := g.GetBranch(ctx, repository, branchID)
		if err != nil {
			return err
		}
		if branchPreOp.StagingToken == branchPostOp.StagingToken {
			break
		}
		// we got a new token, try again
		log.WithFields(logging.Fields{
			"try":               try + 1,
			"branch_token_pre":  branchPreOp.StagingToken,
			"branch_token_post": branchPostOp.StagingToken,
		}).Debug("Retrying Set")
	}
	if try == options.MaxTries {
		return fmt.Errorf("safe branch write: %w", ErrTooManyTries)
	}
	return nil
}

func (g *Graveler) Delete(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error {
	isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
	if err != nil {
		return err
	}
	if isProtected {
		return ErrWriteToProtectedBranch
	}

	log := g.log(ctx).WithFields(logging.Fields{"key": key, "operation": "delete"})
	return g.safeBranchWrite(ctx, log, repository, branchID,
		safeBranchWriteOptions{}, func(branch *Branch) error {
			return g.deleteUnsafe(ctx, repository, branch, key, nil)
		})
}

// DeleteBatch delete batch of keys. Keys length is limited to DeleteKeysMaxSize. Return error can be of type
// 'multi-error' holds DeleteError with each key/error that failed as part of the batch.
func (g *Graveler) DeleteBatch(ctx context.Context, repository *RepositoryRecord, branchID BranchID, keys []Key) error {
	isProtected, err := g.protectedBranchesManager.IsBlocked(ctx, repository, branchID, BranchProtectionBlockedAction_STAGING_WRITE)
	if err != nil {
		return err
	}
	if isProtected {
		return ErrWriteToProtectedBranch
	}
	if len(keys) > DeleteKeysMaxSize {
		return fmt.Errorf("keys length (%d) passed the maximum allowed(%d): %w", len(keys), DeleteKeysMaxSize, ErrInvalidValue)
	}

	var m *multierror.Error
	log := g.log(ctx).WithField("operation", "delete_keys")
	err = g.safeBranchWrite(ctx, log, repository, branchID, safeBranchWriteOptions{}, func(branch *Branch) error {
		var cachedMetaRangeID MetaRangeID // used to cache the committed branch metarange ID
		for _, key := range keys {
			err := g.deleteUnsafe(ctx, repository, branch, key, &cachedMetaRangeID)
			if err != nil {
				m = multierror.Append(m, &DeleteError{Key: key, Err: err})
			}
		}
		return m.ErrorOrNil()
	})
	return err
}

func (g *Graveler) deleteUnsafe(ctx context.Context, repository *RepositoryRecord, branch *Branch, key Key, cachedMetaRangeID *MetaRangeID) error {
	// First attempt to update on staging token
	err := g.StagingManager.Set(ctx, branch.StagingToken, key, nil, true)
	if !errors.Is(err, kv.ErrPredicateFailed) {
		return err
	}

	// check key in committed - do we need tombstone?
	var metaRangeID MetaRangeID
	if cachedMetaRangeID != nil && *cachedMetaRangeID != "" {
		metaRangeID = *cachedMetaRangeID
	} else {
		commit, err := g.RefManager.GetCommit(ctx, repository, branch.CommitID)
		if err != nil {
			return err
		}
		metaRangeID = commit.MetaRangeID
	}

	_, err = g.CommittedManager.Get(ctx, repository.StorageNamespace, metaRangeID, key)
	if err == nil {
		// found in committed, set tombstone
		return g.StagingManager.Set(ctx, branch.StagingToken, key, nil, false)
	}
	if !errors.Is(err, ErrNotFound) {
		// unknown error
		return fmt.Errorf("reading from committed: %w", err)
	}
	// else key is nowhere to be found - continue to staged

	// check staging for entry or tombstone
	val, err := g.getFromStagingArea(ctx, branch, key)
	if err == nil {
		if val == nil {
			// found tombstone in staging, do nothing
			return nil
		}
		// found in staging, set tombstone
		return g.StagingManager.Set(ctx, branch.StagingToken, key, nil, false)
	}
	if !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("reading from staging: %w", err)
	}
	// err == ErrNotFound, key is nowhere to be found - nothing to do
	return nil
}

// ListStaging Exposing listStagingArea to catalog for PrepareGCUncommitted
func (g *Graveler) ListStaging(ctx context.Context, branch *Branch, batchSize int) (ValueIterator, error) {
	return g.listStagingArea(ctx, branch, batchSize)
}

// listStagingArea Returns an iterator which is an aggregation of all changes on all the branch's staging area (staging + sealed)
// for each key in the staging area it will return the latest update for that key (the value that appears in the newest token)
func (g *Graveler) listStagingArea(ctx context.Context, b *Branch, batchSize int) (ValueIterator, error) {
	if b.StagingToken == "" {
		return nil, ErrNotFound
	}
	it, err := g.StagingManager.List(ctx, b.StagingToken, batchSize)
	if err != nil {
		return nil, err
	}

	if len(b.SealedTokens) == 0 { // Only staging token exists -> return its iterator
		return it, nil
	}

	itrs, err := g.listSealedTokens(ctx, b, batchSize)
	if err != nil {
		it.Close()
		return nil, err
	}
	itrs = append([]ValueIterator{it}, itrs...)
	return NewCombinedIterator(itrs...), nil
}

func (g *Graveler) listSealedTokens(ctx context.Context, b *Branch, batchSize int) ([]ValueIterator, error) {
	iterators := make([]ValueIterator, 0, len(b.SealedTokens))
	for _, st := range b.SealedTokens {
		it, err := g.StagingManager.List(ctx, st, batchSize)
		if err != nil {
			// close the iterators we managed to open
			for _, iter := range iterators {
				iter.Close()
			}
			return nil, err
		}
		iterators = append(iterators, it)
	}
	return iterators, nil
}

func (g *Graveler) sealedTokensIterator(ctx context.Context, b *Branch, batchSize int) (ValueIterator, error) {
	itrs, err := g.listSealedTokens(ctx, b, batchSize)
	if err != nil {
		return nil, err
	}
	if len(itrs) == 0 {
		return nil, ErrNoChanges
	}

	changes := NewCombinedIterator(itrs...)
	return changes, nil
}

func (g *Graveler) List(ctx context.Context, repository *RepositoryRecord, ref Ref, batchSize int) (ValueIterator, error) {
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
		stagingList, err := g.listStagingArea(ctx, reference.BranchRecord.Branch, batchSize)
		if err != nil {
			listing.Close()
			return nil, err
		}
		listing = NewFilterTombstoneIterator(NewCombinedIterator(stagingList, listing))
	}

	return listing, nil
}

func (g *Graveler) Commit(ctx context.Context, repository *RepositoryRecord, branchID BranchID, params CommitParams) (CommitID, error) {
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
			changes, err := g.sealedTokensIterator(ctx, branch, 0)
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
		g.log(ctx).WithError(err).
			WithField("run_id", postRunID).
			WithField("pre_run_id", preRunID).
			Error("Post-commit hook failed")
	}
	return newCommitID, nil
}

func (g *Graveler) retryBranchUpdate(ctx context.Context, repository *RepositoryRecord, branchID BranchID, f BranchUpdateFunc) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = BranchUpdateMaxInterval

	try := 1
	err := backoff.Retry(func() error {
		// TODO(eden) issue 3586 - if the branch commit id hasn't changed, update the fields instead of fail
		err := g.RefManager.BranchUpdate(ctx, repository, branchID, f)
		if errors.Is(err, kv.ErrPredicateFailed) && try < BranchUpdateMaxTries {
			g.log(ctx).WithField("try", try).
				WithField("branchID", branchID).
				Info("Retrying update branch")
			try += 1
			return err
		}
		if err != nil {
			return backoff.Permanent(err)
		}
		return nil
	}, bo)
	if err != nil && try >= BranchUpdateMaxTries {
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

func (g *Graveler) AddCommit(ctx context.Context, repository *RepositoryRecord, commit Commit) (CommitID, error) {
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
func (g *Graveler) addCommitNoLock(ctx context.Context, repository *RepositoryRecord, commit Commit) (CommitID, error) {
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

func (g *Graveler) isStagingEmpty(ctx context.Context, repository *RepositoryRecord, branch *Branch) (bool, error) {
	itr, err := g.listStagingArea(ctx, branch, 1)
	if err != nil {
		return false, err
	}
	defer itr.Close()

	// Iterating over staging area (staging + sealed) of the branch and check for entries
	return g.checkEmpty(ctx, repository, branch, itr)
}

// checkEmpty - staging iterator is not considered empty IFF it contains any non-tombstone entry
// or a tombstone entry exists for a key which is already committed
func (g *Graveler) checkEmpty(ctx context.Context, repository *RepositoryRecord, branch *Branch, changesIt ValueIterator) (bool, error) {
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

func (g *Graveler) isSealedEmpty(ctx context.Context, repository *RepositoryRecord, branch *Branch) (bool, error) {
	if len(branch.SealedTokens) == 0 {
		return true, nil
	}
	itrs, err := g.sealedTokensIterator(ctx, branch, 1)
	if err != nil {
		return false, err
	}
	defer itrs.Close()
	return g.checkEmpty(ctx, repository, branch, itrs)
}

// dropTokens deletes all staging area entries of a given branch from store
func (g *Graveler) dropTokens(ctx context.Context, tokens ...StagingToken) {
	for _, token := range tokens {
		err := g.StagingManager.DropAsync(ctx, token)
		if err != nil {
			logging.FromContext(ctx).WithError(err).WithField("staging_token", token).Error("Failed to drop staging token")
		}
	}
}

func (g *Graveler) Reset(ctx context.Context, repository *RepositoryRecord, branchID BranchID) error {
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
func (g *Graveler) resetKey(ctx context.Context, repository *RepositoryRecord, branch *Branch, key Key, stagedValue *Value, st StagingToken) error {
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
		return g.StagingManager.Set(ctx, st, key, committed, false)
		// entry not committed and changed in staging area => override with tombstone
		// If not committed and staging == tombstone => ignore
	} else if !isCommitted && stagedValue != nil {
		return g.StagingManager.Set(ctx, st, key, nil, false)
	}

	return nil
}

func (g *Graveler) ResetKey(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error {
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

func (g *Graveler) ResetPrefix(ctx context.Context, repository *RepositoryRecord, branchID BranchID, key Key) error {
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
		itr, err := g.listStagingArea(ctx, branch, 0)
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
func (g *Graveler) Revert(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref, parentNumber int, commitParams CommitParams) (CommitID, error) {
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
			return "", fmt.Errorf("%w: parent %d", ErrParentOutOfRange, parentNumber)
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
		metaRangeID, err := g.CommittedManager.Merge(ctx, repository.StorageNamespace, branchCommit.MetaRangeID, parentMetaRangeID, commitRecord.MetaRangeID, MergeStrategyNone)
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

// CherryPick creates a new commit on the given branch, with the changes from the given commit.
// If the commit is a merge commit, 'parentNumber' is the parent number (1-based) relative to which the cherry-pick is done.
func (g *Graveler) CherryPick(ctx context.Context, repository *RepositoryRecord, branchID BranchID, ref Ref, parentNumber *int, committer string) (CommitID, error) {
	commitRecord, err := g.dereferenceCommit(ctx, repository, ref)
	if err != nil {
		return "", fmt.Errorf("get commit from ref %s: %w", ref, err)
	}

	pn := 1
	if parentNumber == nil {
		if len(commitRecord.Parents) > 1 {
			return "", ErrCherryPickMergeNoParent
		}
	} else {
		pn = *parentNumber
	}
	if pn > len(commitRecord.Parents) {
		// validate parent is in range:
		return "", fmt.Errorf("parent %d: %w", pn, ErrParentOutOfRange)
	}
	pn--

	if err := g.prepareForCommitIDUpdate(ctx, repository, branchID); err != nil {
		return "", err
	}

	var parentMetaRangeID MetaRangeID
	if len(commitRecord.Parents) > 0 {
		parentCommit, err := g.dereferenceCommit(ctx, repository, commitRecord.Parents[pn].Ref())
		if err != nil {
			return "", fmt.Errorf("get commit from ref %s: %w", commitRecord.Parents[pn], err)
		}
		parentMetaRangeID = parentCommit.MetaRangeID
	}

	var commitID CommitID
	var tokensToDrop []StagingToken
	err = g.RefManager.BranchUpdate(ctx, repository, branchID, func(branch *Branch) (*Branch, error) {
		if empty, err := g.isSealedEmpty(ctx, repository, branch); err != nil {
			return nil, err
		} else if !empty {
			return nil, fmt.Errorf("%s: %w", branchID, ErrDirtyBranch)
		}

		branchCommit, err := g.dereferenceCommit(ctx, repository, branch.CommitID.Ref())
		if err != nil {
			return nil, fmt.Errorf("get commit from ref %s: %w", branch.CommitID, err)
		}
		// merge from the parent to the top of the branch, with the given ref as the merge base:
		metaRangeID, err := g.CommittedManager.Merge(ctx, repository.StorageNamespace, branchCommit.MetaRangeID, commitRecord.MetaRangeID, parentMetaRangeID, MergeStrategyNone)
		if err != nil {
			if !errors.Is(err, ErrUserVisible) {
				err = fmt.Errorf("merge: %w", err)
			}
			return nil, err
		}
		commit := NewCommit()
		commit.Committer = committer
		commit.Message = commitRecord.Message
		commit.MetaRangeID = metaRangeID
		commit.Parents = []CommitID{branch.CommitID}
		commit.Generation = branchCommit.Generation + 1

		commit.Metadata = commitRecord.Metadata
		if commit.Metadata == nil {
			commit.Metadata = make(map[string]string)
		}
		commit.Metadata["cherry-pick-origin"] = string(commitRecord.CommitID)
		commit.Metadata["cherry-pick-committer"] = commitRecord.Committer

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

func (g *Graveler) Merge(ctx context.Context, repository *RepositoryRecord, destination BranchID, source Ref, commitParams CommitParams, strategy string) (CommitID, error) {
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
	err := g.retryBranchUpdate(ctx, repository, destination, func(branch *Branch) (*Branch, error) {
		empty, err := g.isSealedEmpty(ctx, repository, branch)
		if err != nil {
			return nil, fmt.Errorf("check if staging empty: %w", err)
		}
		if !empty {
			return nil, fmt.Errorf("%s: %w", destination, ErrDirtyBranch)
		}
		fromCommit, toCommit, baseCommit, err := g.FindMergeBase(ctx, repository, source, Ref(destination))
		if err != nil {
			return nil, err
		}
		g.log(ctx).WithFields(logging.Fields{
			"repository":             repository.RepositoryID,
			"source":                 source,
			"destination":            destination,
			"source_meta_range":      fromCommit.MetaRangeID,
			"destination_meta_range": toCommit.MetaRangeID,
			"base_meta_range":        baseCommit.MetaRangeID,
			"strategy":               strategy,
		}).Trace("Merge")

		var mergeStrategy MergeStrategy
		switch strategy {
		case MergeStrategyDestWinsStr:
			mergeStrategy = MergeStrategyDest
		case MergeStrategySrcWinsStr:
			mergeStrategy = MergeStrategySrc
		case "":
			mergeStrategy = MergeStrategyNone
		default:
			return nil, ErrInvalidMergeStrategy
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
		commit.Metadata[MergeStrategyMetadataKey] = mergeStrategyString[mergeStrategy]
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

		SourceRef: commitID.Ref(),
		Commit:    commit,
		CommitID:  commitID,
		PreRunID:  preRunID,
	})
	if err != nil {
		g.log(ctx).
			WithError(err).
			WithField("run_id", postRunID).
			WithField("pre_run_id", preRunID).
			Error("Post-merge hook failed")
	}
	return commitID, nil
}

func (g *Graveler) retryRepoMetadataUpdate(ctx context.Context, repository *RepositoryRecord, f RepoMetadataUpdateFunc) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = RepoMetadataUpdateMaxInterval
	bo.MaxElapsedTime = RepoMetadataUpdateMaxElapsedTime
	bo.RandomizationFactor = RepoMetadataUpdateRandomFactor
	logger := g.log(ctx).WithField("repository_id", repository.RepositoryID)

	try := 1
	err := backoff.Retry(func() error {
		err := g.RefManager.SetRepositoryMetadata(ctx, repository, f)
		if errors.Is(err, kv.ErrPredicateFailed) {
			logger.WithField("try", try).
				Info("Retrying update repo metadata")
			try += 1
			return err
		}
		if err != nil {
			return backoff.Permanent(err)
		}
		return nil
	}, bo)
	if errors.Is(err, kv.ErrPredicateFailed) {
		return fmt.Errorf("update repo metadata: %w", ErrTooManyTries)
	}
	return err
}

func (g *Graveler) Import(ctx context.Context, repository *RepositoryRecord, destination BranchID, source MetaRangeID, commitParams CommitParams) (CommitID, error) {
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
	err := g.retryBranchUpdate(ctx, repository, destination, func(branch *Branch) (*Branch, error) {
		empty, err := g.isSealedEmpty(ctx, repository, branch)
		if err != nil {
			return nil, fmt.Errorf("is staging empty %s: %w", destination, err)
		}
		if !empty {
			return nil, fmt.Errorf("%s: %w", destination, ErrDirtyBranch)
		}
		toCommit, err := g.dereferenceCommit(ctx, repository, Ref(destination))
		if err != nil {
			return nil, err
		}

		g.log(ctx).WithFields(logging.Fields{
			"repository":             repository.RepositoryID,
			"destination":            destination,
			"source_meta_range":      source,
			"destination_meta_range": toCommit.MetaRangeID,
		}).Trace("Import")

		metaRangeID, err := g.CommittedManager.Merge(ctx, storageNamespace, toCommit.MetaRangeID, source, "", MergeStrategySrc)
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
		commit.Parents = []CommitID{toCommit.CommitID}
		commit.Generation = toCommit.Generation + 1
		commit.Metadata = commitParams.Metadata
		commit.Metadata[MergeStrategyMetadataKey] = MergeStrategySrcWinsStr
		preRunID = g.hooks.NewRunID()
		err = g.hooks.PreCommitHook(ctx, HookRecord{
			RunID:            preRunID,
			EventType:        EventTypePreCommit,
			SourceRef:        destination.Ref(),
			RepositoryID:     repository.RepositoryID,
			StorageNamespace: storageNamespace,
			BranchID:         destination,
			Commit:           commit,
		})
		if err != nil {
			return nil, &HookAbortError{
				EventType: EventTypePreCommit,
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
	err = g.hooks.PostCommitHook(ctx, HookRecord{
		EventType:        EventTypePostCommit,
		RunID:            postRunID,
		RepositoryID:     repository.RepositoryID,
		StorageNamespace: storageNamespace,
		SourceRef:        commitID.Ref(),
		BranchID:         destination,
		Commit:           commit,
		CommitID:         commitID,
		PreRunID:         preRunID,
	})
	if err != nil {
		g.log(ctx).WithError(err).
			WithField("run_id", postRunID).
			WithField("pre_run_id", preRunID).
			Error("Post-commit hook failed")
	}

	if err = g.retryRepoMetadataUpdate(ctx, repository, func(metadata RepositoryMetadata) (RepositoryMetadata, error) {
		metadata[MetadataKeyLastImportTimeStamp] = strconv.FormatInt(commit.CreationDate.Unix(), 10)
		return metadata, nil
	}); err != nil {
		g.log(ctx).WithField("repository_id", repository.RepositoryID).WithError(err).Error("Failed to update import metadata")
	}

	return commitID, nil
}

// DiffUncommitted returns DiffIterator between committed data and staging area of a branch
func (g *Graveler) DiffUncommitted(ctx context.Context, repository *RepositoryRecord, branchID BranchID) (DiffIterator, error) {
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

	valueIterator, err := g.listStagingArea(ctx, branch, 0)
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
//
//	will return an error if 'ref' points to an explicit staging area
func (g *Graveler) dereferenceCommit(ctx context.Context, repository *RepositoryRecord, ref Ref) (*CommitRecord, error) {
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

func (g *Graveler) Diff(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error) {
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
	stagingIterator, err := g.listStagingArea(ctx, rightBranch, 0)
	if err != nil {
		leftValueIterator.Close()
		return nil, err
	}
	return NewCombinedDiffIterator(diff, leftValueIterator, stagingIterator), nil
}

func (g *Graveler) FindMergeBase(ctx context.Context, repository *RepositoryRecord, from Ref, to Ref) (*CommitRecord, *CommitRecord, *Commit, error) {
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

func (g *Graveler) Compare(ctx context.Context, repository *RepositoryRecord, left, right Ref) (DiffIterator, error) {
	fromCommit, toCommit, baseCommit, err := g.FindMergeBase(ctx, repository, right, left)
	if err != nil {
		return nil, err
	}
	return g.CommittedManager.Compare(ctx, repository.StorageNamespace, toCommit.MetaRangeID, fromCommit.MetaRangeID, baseCommit.MetaRangeID)
}

func (g *Graveler) SetHooksHandler(handler HooksHandler) {
	if handler == nil {
		g.hooks = &HooksNoOp{}
	} else {
		g.hooks = handler
	}
}

func (g *Graveler) LoadCommits(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error {
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

func (g *Graveler) LoadBranches(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error {
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

func (g *Graveler) LoadTags(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) error {
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

func (g *Graveler) GetMetaRange(ctx context.Context, repository *RepositoryRecord, metaRangeID MetaRangeID) (MetaRangeAddress, error) {
	return g.CommittedManager.GetMetaRange(ctx, repository.StorageNamespace, metaRangeID)
}

func (g *Graveler) GetRange(ctx context.Context, repository *RepositoryRecord, rangeID RangeID) (RangeAddress, error) {
	return g.CommittedManager.GetRange(ctx, repository.StorageNamespace, rangeID)
}

func (g *Graveler) DumpCommits(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error) {
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

func (g *Graveler) DumpBranches(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error) {
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

func (g *Graveler) DumpTags(ctx context.Context, repository *RepositoryRecord) (*MetaRangeID, error) {
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

func (g *Graveler) SetLinkAddress(ctx context.Context, repository *RepositoryRecord, token string) error {
	return g.RefManager.SetLinkAddress(ctx, repository, token)
}

func (g *Graveler) VerifyLinkAddress(ctx context.Context, repository *RepositoryRecord, token string) error {
	return g.RefManager.VerifyLinkAddress(ctx, repository, token)
}

func (g *Graveler) ListLinkAddresses(ctx context.Context, repository *RepositoryRecord) (AddressTokenIterator, error) {
	return g.RefManager.ListLinkAddresses(ctx, repository)
}

func (g *Graveler) DeleteExpiredLinkAddresses(ctx context.Context, repository *RepositoryRecord) error {
	return g.RefManager.DeleteExpiredLinkAddresses(ctx, repository)
}

func (g *Graveler) IsLinkAddressExpired(token *LinkAddressData) (bool, error) {
	return g.RefManager.IsLinkAddressExpired(token)
}

func (g *Graveler) DeleteExpiredImports(ctx context.Context, repository *RepositoryRecord) error {
	return g.RefManager.DeleteExpiredImports(ctx, repository)
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
	SaveGarbageCollectionUncommitted(ctx context.Context, repository *RepositoryRecord, filename, runID string) error
	GetUncommittedLocation(runID string, sn StorageNamespace) (string, error)
	GetAddressesLocation(sn StorageNamespace) (string, error)
	NewID() string
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
