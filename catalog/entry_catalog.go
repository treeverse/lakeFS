package catalog

import (
	"context"
	"crypto"
	_ "crypto/sha256"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/ref"
	"github.com/treeverse/lakefs/graveler/sstable"
	"github.com/treeverse/lakefs/graveler/staging"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/pyramid"
	"github.com/treeverse/lakefs/pyramid/params"
)

// hashAlg is the hashing algorithm to use to generate graveler identifiers.  Changing it
// causes all old identifiers to change, so while existing installations will continue to
// function they will be unable to re-use any existing objects.
const hashAlg = crypto.SHA256

type Path string

type EntryRecord struct {
	Path Path
	*Entry
}

type EntryListing struct {
	CommonPrefix bool
	Path
	*Entry
}

type EntryDiff struct {
	Type  graveler.DiffType
	Path  Path
	Entry *Entry
}

type EntryIterator interface {
	Next() bool
	SeekGE(id Path)
	Value() *EntryRecord
	Err() error
	Close()
}

type EntryListingIterator interface {
	Next() bool
	SeekGE(id Path)
	Value() *EntryListing
	Err() error
	Close()
}

type EntryDiffIterator interface {
	Next() bool
	SeekGE(id Path)
	Value() *EntryDiff
	Err() error
	Close()
}

func (id Path) String() string {
	return string(id)
}

type Store interface {
	graveler.KeyValueStore
	graveler.VersionController
	graveler.Dumper
	graveler.Plumbing
	graveler.Loader
}

type Actions interface {
	Run(ctx context.Context, event actions.Event, deps actions.Deps) error
}

type EntryCatalog struct {
	Actions      Actions
	BlockAdapter block.Adapter
	Store        Store
}

const (
	RangeFSName     = "range"
	MetaRangeFSName = "meta-range"
)

type Config struct {
	Config  *config.Config
	DB      db.Database
	LockDB  db.Database
	Actions Actions
}

func NewEntryCatalog(cfg Config) (*EntryCatalog, error) {
	if cfg.LockDB == nil {
		cfg.LockDB = cfg.DB
	}

	tierFSParams, err := cfg.Config.GetCommittedTierFSParams()
	if err != nil {
		return nil, fmt.Errorf("configure tiered FS for committed: %w", err)
	}
	metaRangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              MetaRangeFSName,
		DiskAllocProportion: tierFSParams.MetaRangeAllocationProportion,
	})
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed metaranges: %w", err)
	}

	rangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              RangeFSName,
		DiskAllocProportion: tierFSParams.RangeAllocationProportion,
	})
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed ranges: %w", err)
	}

	pebbleSSTableCache := pebble.NewCache(tierFSParams.PebbleSSTableCacheSizeBytes)
	defer pebbleSSTableCache.Unref()

	sstableManager := sstable.NewPebbleSSTableRangeManager(pebbleSSTableCache, rangeFS, hashAlg)
	sstableMetaManager := sstable.NewPebbleSSTableRangeManager(pebbleSSTableCache, metaRangeFS, hashAlg)
	sstableMetaRangeManager, err := committed.NewMetaRangeManager(
		*cfg.Config.GetCommittedParams(),
		// TODO(ariels): Use separate range managers for metaranges and ranges
		sstableMetaManager,
		sstableManager,
	)
	if err != nil {
		return nil, fmt.Errorf("create SSTable-based metarange manager: %w", err)
	}
	committedManager := committed.NewCommittedManager(sstableMetaRangeManager)

	stagingManager := staging.NewManager(cfg.DB)
	refManager := ref.NewPGRefManager(cfg.DB, ident.NewHexAddressProvider())
	branchLocker := ref.NewBranchLocker(cfg.LockDB)
	store := graveler.NewGraveler(branchLocker, committedManager, stagingManager, refManager)
	entryCatalog := &EntryCatalog{
		BlockAdapter: tierFSParams.Adapter,
		Store:        store,
		Actions:      cfg.Actions,
	}
	store.SetHooksHandler(entryCatalog)
	return entryCatalog, nil
}

func (e *EntryCatalog) AddCommitToBranchHead(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, commit graveler.Commit) (graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return "", err
	}
	return e.Store.AddCommitToBranchHead(ctx, repositoryID, branchID, commit)
}

func (e *EntryCatalog) AddCommit(ctx context.Context, repositoryID graveler.RepositoryID, commit graveler.Commit) (graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return "", err
	}
	return e.Store.AddCommit(ctx, repositoryID, commit)
}

func (e *EntryCatalog) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.Repository, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	return e.Store.GetRepository(ctx, repositoryID)
}

func (e *EntryCatalog) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageNamespace graveler.StorageNamespace, branchID graveler.BranchID) (*graveler.Repository, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"storageNamespace", storageNamespace, ValidateStorageNamespace},
	}); err != nil {
		return nil, err
	}
	return e.Store.CreateRepository(ctx, repositoryID, storageNamespace, branchID)
}

func (e *EntryCatalog) CreateBareRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageNamespace graveler.StorageNamespace, defaultBranchID graveler.BranchID) (*graveler.Repository, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"storageNamespace", storageNamespace, ValidateStorageNamespace},
	}); err != nil {
		return nil, err
	}
	return e.Store.CreateBareRepository(ctx, repositoryID, storageNamespace, defaultBranchID)
}

func (e *EntryCatalog) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	return e.Store.ListRepositories(ctx)
}

func (e *EntryCatalog) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return err
	}
	return e.Store.DeleteRepository(ctx, repositoryID)
}

func (e *EntryCatalog) CreateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"ref", ref, ValidateRef},
	}); err != nil {
		return nil, err
	}
	return e.Store.CreateBranch(ctx, repositoryID, branchID, ref)
}

func (e *EntryCatalog) UpdateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"ref", ref, ValidateRef},
	}); err != nil {
		return nil, err
	}
	return e.Store.UpdateBranch(ctx, repositoryID, branchID, ref)
}

func (e *EntryCatalog) GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return nil, err
	}
	return e.Store.GetBranch(ctx, repositoryID, branchID)
}

func (e *EntryCatalog) GetTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) (*graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"tagID", tagID, ValidateTagID},
	}); err != nil {
		return nil, err
	}
	return e.Store.GetTag(ctx, repositoryID, tagID)
}

func (e *EntryCatalog) CreateTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID, commitID graveler.CommitID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"tagID", tagID, ValidateTagID},
		{"commitID", commitID, ValidateCommitID},
	}); err != nil {
		return err
	}
	return e.Store.CreateTag(ctx, repositoryID, tagID, commitID)
}

func (e *EntryCatalog) DeleteTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"tagID", tagID, ValidateTagID},
	}); err != nil {
		return err
	}
	return e.Store.DeleteTag(ctx, repositoryID, tagID)
}

func (e *EntryCatalog) ListTags(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.TagIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	it, err := e.Store.ListTags(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	return it, nil
}

func (e *EntryCatalog) Log(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (graveler.CommitIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"commitID", commitID, ValidateCommitID},
	}); err != nil {
		return nil, err
	}
	return e.Store.Log(ctx, repositoryID, commitID)
}

func (e *EntryCatalog) ListBranches(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.BranchIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	return e.Store.ListBranches(ctx, repositoryID)
}

func (e *EntryCatalog) DeleteBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return err
	}
	return e.Store.DeleteBranch(ctx, repositoryID, branchID)
}

func (e *EntryCatalog) WriteMetaRange(ctx context.Context, repositoryID graveler.RepositoryID, it EntryIterator) (*graveler.MetaRangeID, error) {
	return e.Store.WriteMetaRange(ctx, repositoryID, NewEntryToValueIterator(it))
}

func (e *EntryCatalog) Commit(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, commitParams graveler.CommitParams) (graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return "", err
	}
	return e.Store.Commit(ctx, repositoryID, branchID, commitParams)
}

func (e *EntryCatalog) GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"commitID", commitID, ValidateCommitID},
	}); err != nil {
		return nil, err
	}
	return e.Store.GetCommit(ctx, repositoryID, commitID)
}

func (e *EntryCatalog) Dereference(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref) (graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"ref", ref, ValidateRef},
	}); err != nil {
		return "", err
	}
	return e.Store.Dereference(ctx, repositoryID, ref)
}

func (e *EntryCatalog) Reset(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return err
	}
	return e.Store.Reset(ctx, repositoryID, branchID)
}

func (e *EntryCatalog) ResetKey(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"path", path, ValidatePath},
	}); err != nil {
		return err
	}
	key := graveler.Key(path)
	return e.Store.ResetKey(ctx, repositoryID, branchID, key)
}

func (e *EntryCatalog) ResetPrefix(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, prefix Path) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return err
	}
	keyPrefix := graveler.Key(prefix)
	return e.Store.ResetPrefix(ctx, repositoryID, branchID, keyPrefix)
}

func (e *EntryCatalog) Revert(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref, parentNumber int, commitParams graveler.CommitParams) (graveler.CommitID, graveler.DiffSummary, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"ref", ref, ValidateRef},
		{"committer", commitParams.Committer, ValidateRequiredString},
		{"message", commitParams.Message, ValidateRequiredString},
		{"parentNumber", parentNumber, ValidateNonNegativeInt},
	}); err != nil {
		return "", graveler.DiffSummary{}, err
	}
	return e.Store.Revert(ctx, repositoryID, branchID, ref, parentNumber, commitParams)
}

func (e *EntryCatalog) Merge(ctx context.Context, repositoryID graveler.RepositoryID, destination graveler.BranchID, source graveler.Ref, commitParams graveler.CommitParams) (graveler.CommitID, graveler.DiffSummary, error) {
	if commitParams.Message == "" {
		commitParams.Message = fmt.Sprintf("Merge '%s' into '%s'", source, destination)
	}
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"destination", destination, ValidateBranchID},
		{"source", source, ValidateRef},
		{"committer", commitParams.Committer, ValidateRequiredString},
		{"message", commitParams.Message, ValidateRequiredString},
	}); err != nil {
		return "", graveler.DiffSummary{}, err
	}
	return e.Store.Merge(ctx, repositoryID, destination, source, commitParams)
}

func (e *EntryCatalog) DiffUncommitted(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (EntryDiffIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return nil, err
	}
	iter, err := e.Store.DiffUncommitted(ctx, repositoryID, branchID)
	if err != nil {
		return nil, err
	}
	return NewEntryDiffIterator(iter), nil
}

func (e *EntryCatalog) Diff(ctx context.Context, repositoryID graveler.RepositoryID, left, right graveler.Ref) (EntryDiffIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"left", left, ValidateRef},
		{"right", right, ValidateRef},
	}); err != nil {
		return nil, err
	}
	iter, err := e.Store.Diff(ctx, repositoryID, left, right)
	if err != nil {
		return nil, err
	}
	return NewEntryDiffIterator(iter), nil
}

func (e *EntryCatalog) Compare(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.Ref, to graveler.Ref) (EntryDiffIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"from", from, ValidateRef},
		{"to", to, ValidateRef},
	}); err != nil {
		return nil, err
	}
	iter, err := e.Store.Compare(ctx, repositoryID, from, to)
	if err != nil {
		return nil, err
	}
	return NewEntryDiffIterator(iter), nil
}

func (e *EntryCatalog) GetEntry(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, path Path) (*Entry, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"ref", ref, ValidateRef},
		{"path", path, ValidatePath},
	}); err != nil {
		return nil, err
	}
	val, err := e.Store.Get(ctx, repositoryID, ref, graveler.Key(path))
	if err != nil {
		return nil, err
	}
	return ValueToEntry(val)
}

func (e *EntryCatalog) SetEntry(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path, entry *Entry) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"path", path, ValidatePath},
	}); err != nil {
		return err
	}
	key := graveler.Key(path)
	value, err := EntryToValue(entry)
	if err != nil {
		return err
	}
	return e.Store.Set(ctx, repositoryID, branchID, key, *value)
}

func (e *EntryCatalog) DeleteEntry(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"path", path, ValidatePath},
	}); err != nil {
		return err
	}
	key := graveler.Key(path)
	return e.Store.Delete(ctx, repositoryID, branchID, key)
}

func (e *EntryCatalog) ListEntries(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, prefix, delimiter Path) (EntryListingIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"ref", ref, ValidateRef},
		{"prefix", prefix, ValidatePathOptional},
		{"delimiter", delimiter, ValidatePathOptional},
	}); err != nil {
		return nil, err
	}
	iter, err := e.Store.List(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	it := NewValueToEntryIterator(iter)
	return NewEntryListingIterator(it, prefix, delimiter), nil
}

func (e *EntryCatalog) DumpCommits(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.MetaRangeID, error) {
	return e.Store.DumpCommits(ctx, repositoryID)
}

func (e *EntryCatalog) DumpBranches(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.MetaRangeID, error) {
	return e.Store.DumpBranches(ctx, repositoryID)
}

func (e *EntryCatalog) DumpTags(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.MetaRangeID, error) {
	return e.Store.DumpTags(ctx, repositoryID)
}

func (e *EntryCatalog) LoadCommits(ctx context.Context, repositoryID graveler.RepositoryID, metaRangeID graveler.MetaRangeID) error {
	return e.Store.LoadCommits(ctx, repositoryID, metaRangeID)
}

func (e *EntryCatalog) LoadBranches(ctx context.Context, repositoryID graveler.RepositoryID, metaRangeID graveler.MetaRangeID) error {
	return e.Store.LoadBranches(ctx, repositoryID, metaRangeID)
}

func (e *EntryCatalog) LoadTags(ctx context.Context, repositoryID graveler.RepositoryID, metaRangeID graveler.MetaRangeID) error {
	return e.Store.LoadTags(ctx, repositoryID, metaRangeID)
}

func (e *EntryCatalog) PreCommitHook(ctx context.Context, eventID uuid.UUID, repositoryRecord graveler.RepositoryRecord, branch graveler.BranchID, commit graveler.Commit) error {
	evt := actions.Event{
		EventType:     actions.EventTypePreCommit,
		EventID:       eventID,
		EventTime:     time.Now(),
		RepositoryID:  repositoryRecord.RepositoryID.String(),
		BranchID:      branch.String(),
		CommitMessage: commit.Message,
		Committer:     commit.Committer,
		Metadata:      commit.Metadata,
	}
	deps := actions.Deps{
		Source: &actionsSource{
			catalog:          e,
			adapter:          e.BlockAdapter,
			repositoryID:     repositoryRecord.RepositoryID,
			storageNamespace: repositoryRecord.StorageNamespace,
			ref:              branch.Ref(),
		},
		Output: &actionsWriter{
			adapter:          e.BlockAdapter,
			storageNamespace: repositoryRecord.StorageNamespace,
		},
	}
	return e.Actions.Run(ctx, evt, deps)
}

func (e *EntryCatalog) PostCommitHook(ctx context.Context, eventID uuid.UUID, repositoryRecord graveler.RepositoryRecord, branch graveler.BranchID, commitRecord graveler.CommitRecord) error {
	return nil
}

func (e *EntryCatalog) PreMergeHook(ctx context.Context, eventID uuid.UUID, repositoryRecord graveler.RepositoryRecord, destination graveler.BranchID, source graveler.Ref, commit graveler.Commit) error {
	evt := actions.Event{
		EventType:     actions.EventTypePreMerge,
		EventID:       eventID,
		EventTime:     time.Now(),
		RepositoryID:  repositoryRecord.RepositoryID.String(),
		BranchID:      destination.String(),
		SourceRef:     source.String(),
		CommitMessage: commit.Message,
		Committer:     commit.Committer,
		Metadata:      commit.Metadata,
	}
	deps := actions.Deps{
		Source: &actionsSource{
			catalog:          e,
			adapter:          e.BlockAdapter,
			repositoryID:     repositoryRecord.RepositoryID,
			storageNamespace: repositoryRecord.StorageNamespace,
			ref:              source,
		},
		Output: &actionsWriter{
			adapter:          e.BlockAdapter,
			storageNamespace: repositoryRecord.StorageNamespace,
		},
	}
	return e.Actions.Run(ctx, evt, deps)
}

func (e *EntryCatalog) PostMergeHook(ctx context.Context, eventID uuid.UUID, repositoryRecord graveler.RepositoryRecord, destination graveler.BranchID, source graveler.Ref, commitRecord graveler.CommitRecord) error {
	return nil
}

func (e *EntryCatalog) GetMetaRange(ctx context.Context, repositoryID graveler.RepositoryID, metaRangeID graveler.MetaRangeID) (graveler.MetaRangeData, error) {
	return e.Store.GetMetaRange(ctx, repositoryID, metaRangeID)
}

func (e *EntryCatalog) GetRange(ctx context.Context, repositoryID graveler.RepositoryID, rangeID graveler.RangeID) (graveler.RangeData, error) {
	return e.Store.GetRange(ctx, repositoryID, rangeID)
}
