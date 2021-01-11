package rocks

import (
	"context"
	"crypto"
	_ "crypto/sha256"
	"fmt"

	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/ref"
	"github.com/treeverse/lakefs/graveler/sstable"
	"github.com/treeverse/lakefs/graveler/staging"
	"github.com/treeverse/lakefs/pyramid"
	"github.com/treeverse/lakefs/pyramid/params"

	pebble "github.com/cockroachdb/pebble"
	pebble_sst "github.com/cockroachdb/pebble/sstable"
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

type EntryCatalog struct {
	store graveler.Graveler
}

const (
	RangeFSName     = "range"
	MetaRangeFSName = "meta-range"
)

func NewEntryCatalog(cfg *config.Config, db db.Database) (*EntryCatalog, error) {
	tierFSParams, err := cfg.GetCommittedTierFSParams()
	if err != nil {
		return nil, fmt.Errorf("configure tiered FS for committed: %w", err)
	}
	metaRangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              MetaRangeFSName,
		DiskAllocProportion: tierFSParams.MetaRangeAllocationProportion,
	})
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed meta-range: %w", err)
	}

	rangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              RangeFSName,
		DiskAllocProportion: tierFSParams.RangeAllocationProportion,
	})
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed meta-range: %w", err)
	}

	pebbleSSTableCache := pebble.NewCache(tierFSParams.PebbleSSTableCacheSizeBytes)
	metaRangeCache := sstable.NewCache(*cfg.GetCommittedRangeSSTableCacheParams(),
		metaRangeFS,
		pebble_sst.ReaderOptions{Cache: pebbleSSTableCache})
	pebbleSSTableCache.Ref() // rangeCache keeps a second reference.
	rangeCache := sstable.NewCache(*cfg.GetCommittedMetaRangeSSTableCacheParams(),
		rangeFS,
		pebble_sst.ReaderOptions{Cache: pebbleSSTableCache})

	sstableManager := sstable.NewPebbleSSTableRangeManager(rangeCache, rangeFS, hashAlg)
	sstableMetaManager := sstable.NewPebbleSSTableRangeManager(metaRangeCache, metaRangeFS, hashAlg)
	sstableMetaRangeManager := committed.NewMetaRangeManager(
		*cfg.GetCommittedParams(),
		// TODO(ariels): Use separate range managers for metaranges and ranges
		sstableMetaManager,
		sstableManager,
	)
	committedManager := committed.NewCommittedManager(sstableMetaRangeManager)

	stagingManager := staging.NewManager(db)
	refManager := ref.NewPGRefManager(db)

	return &EntryCatalog{
		store: graveler.NewGraveler(committedManager, stagingManager, refManager),
	}, nil
}

func (e *EntryCatalog) CommitExistingMetaRange(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, metaRangeID graveler.MetaRangeID, committer string, message string, metadata graveler.Metadata) (graveler.CommitID, error) {
	return e.store.CommitExistingMetaRange(ctx, repositoryID, branchID, metaRangeID, committer, message, metadata)
}

func (e *EntryCatalog) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.Repository, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	return e.store.GetRepository(ctx, repositoryID)
}

func (e *EntryCatalog) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageNamespace graveler.StorageNamespace, branchID graveler.BranchID) (*graveler.Repository, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"storageNamespace", storageNamespace, ValidateStorageNamespace},
	}); err != nil {
		return nil, err
	}
	return e.store.CreateRepository(ctx, repositoryID, storageNamespace, branchID)
}

func (e *EntryCatalog) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	return e.store.ListRepositories(ctx)
}

func (e *EntryCatalog) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return err
	}
	return e.store.DeleteRepository(ctx, repositoryID)
}

func (e *EntryCatalog) CreateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"ref", ref, ValidateRef},
	}); err != nil {
		return nil, err
	}
	return e.store.CreateBranch(ctx, repositoryID, branchID, ref)
}

func (e *EntryCatalog) UpdateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
		{"ref", ref, ValidateRef},
	}); err != nil {
		return nil, err
	}
	return e.store.UpdateBranch(ctx, repositoryID, branchID, ref)
}

func (e *EntryCatalog) GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return nil, err
	}
	return e.store.GetBranch(ctx, repositoryID, branchID)
}

func (e *EntryCatalog) GetTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) (*graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"tagID", tagID, ValidateTagID},
	}); err != nil {
		return nil, err
	}
	return e.store.GetTag(ctx, repositoryID, tagID)
}

func (e *EntryCatalog) CreateTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID, commitID graveler.CommitID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"tagID", tagID, ValidateTagID},
		{"commitID", commitID, ValidateCommitID},
	}); err != nil {
		return err
	}
	return e.store.CreateTag(ctx, repositoryID, tagID, commitID)
}

func (e *EntryCatalog) DeleteTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"tagID", tagID, ValidateTagID},
	}); err != nil {
		return err
	}
	return e.store.DeleteTag(ctx, repositoryID, tagID)
}

func (e *EntryCatalog) ListTags(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.TagID) (graveler.TagIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"from", from, ValidateTagIDOptional},
	}); err != nil {
		return nil, err
	}
	it, err := e.store.ListTags(ctx, repositoryID)
	if err != nil {
		return nil, err
	}
	if from != "" {
		it.SeekGE(from)
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
	return e.store.Log(ctx, repositoryID, commitID)
}

func (e *EntryCatalog) ListBranches(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.BranchIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
	}); err != nil {
		return nil, err
	}
	return e.store.ListBranches(ctx, repositoryID)
}

func (e *EntryCatalog) DeleteBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return err
	}
	return e.store.DeleteBranch(ctx, repositoryID, branchID)
}

func (e *EntryCatalog) WriteMetaRange(ctx context.Context, repositoryID graveler.RepositoryID, it EntryIterator) (*graveler.MetaRangeID, error) {
	return e.store.WriteMetaRange(ctx, repositoryID, NewEntryToValueIterator(it))
}

func (e *EntryCatalog) Commit(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, committer string, message string, metadata graveler.Metadata) (graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return "", err
	}
	return e.store.Commit(ctx, repositoryID, branchID, committer, message, metadata)
}

func (e *EntryCatalog) GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"commitID", commitID, ValidateCommitID},
	}); err != nil {
		return nil, err
	}
	return e.store.GetCommit(ctx, repositoryID, commitID)
}

func (e *EntryCatalog) Dereference(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref) (graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"ref", ref, ValidateRef},
	}); err != nil {
		return "", err
	}
	return e.store.Dereference(ctx, repositoryID, ref)
}

func (e *EntryCatalog) Reset(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return err
	}
	return e.store.Reset(ctx, repositoryID, branchID)
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
	return e.store.ResetKey(ctx, repositoryID, branchID, key)
}

func (e *EntryCatalog) ResetPrefix(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, prefix Path) error {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return err
	}
	keyPrefix := graveler.Key(prefix)
	return e.store.ResetPrefix(ctx, repositoryID, branchID, keyPrefix)
}

func (e *EntryCatalog) Revert(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return "", err
	}
	return e.store.Revert(ctx, repositoryID, branchID, ref)
}

func (e *EntryCatalog) Merge(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.Ref, to graveler.BranchID, committer string, message string, metadata graveler.Metadata) (graveler.CommitID, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"from", from, ValidateRef},
		{"to", to, ValidateBranchID},
		{"committer", committer, ValidateRequiredString},
		{"message", message, ValidateRequiredString},
	}); err != nil {
		return "", err
	}
	return e.store.Merge(ctx, repositoryID, from, to, committer, message, metadata)
}

func (e *EntryCatalog) DiffUncommitted(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (EntryDiffIterator, error) {
	if err := Validate([]ValidateArg{
		{"repositoryID", repositoryID, ValidateRepositoryID},
		{"branchID", branchID, ValidateBranchID},
	}); err != nil {
		return nil, err
	}
	iter, err := e.store.DiffUncommitted(ctx, repositoryID, branchID)
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
	iter, err := e.store.Diff(ctx, repositoryID, left, right)
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
	val, err := e.store.Get(ctx, repositoryID, ref, graveler.Key(path))
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
	return e.store.Set(ctx, repositoryID, branchID, key, *value)
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
	return e.store.Delete(ctx, repositoryID, branchID, key)
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
	iter, err := e.store.List(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	it := NewValueToEntryIterator(iter)
	return NewEntryListingIterator(it, prefix, delimiter), nil
}
