package rocks

import (
	"context"
	"fmt"

	"github.com/treeverse/lakefs/pyramid/params"

	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/ref"
	"github.com/treeverse/lakefs/graveler/staging"
	"github.com/treeverse/lakefs/pyramid"
)

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

func NewPath(id string) (Path, error) {
	return Path(id), nil
}

func (id Path) String() string {
	return string(id)
}

type EntryCatalog struct {
	store graveler.Graveler
}

func NewEntryCatalog(cfg *config.Config, db db.Database) (*EntryCatalog, error) {
	tierFSParams, err := cfg.GetCommittedTierFSParams()
	if err != nil {
		return nil, fmt.Errorf("configure tiered FS for committed: %w", err)
	}
	metaRangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              "meta-range",
		DiskAllocProportion: tierFSParams.MetaRangeAllocationProportion,
	})
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed meta-range: %w", err)
	}

	rangeFS, err := pyramid.NewFS(&params.InstanceParams{
		SharedParams:        tierFSParams.SharedParams,
		FSName:              "range",
		DiskAllocProportion: tierFSParams.RangeAllocationProportion,
	})
	if err != nil {
		return nil, fmt.Errorf("create tiered FS for committed meta-range: %w", err)
	}

	_ = metaRangeFS // silence error
	_ = rangeFS     // silence error

	// TODO(ariels): Create a CommittedManager on top of fs.
	var committedManager graveler.CommittedManager

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
	return e.store.GetRepository(ctx, repositoryID)
}

func (e *EntryCatalog) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageNamespace graveler.StorageNamespace, branchID graveler.BranchID) (*graveler.Repository, error) {
	return e.store.CreateRepository(ctx, repositoryID, storageNamespace, branchID)
}

func (e *EntryCatalog) ListRepositories(ctx context.Context) (graveler.RepositoryIterator, error) {
	return e.store.ListRepositories(ctx)
}

func (e *EntryCatalog) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	return e.store.DeleteRepository(ctx, repositoryID)
}

func (e *EntryCatalog) CreateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	return e.store.CreateBranch(ctx, repositoryID, branchID, ref)
}

func (e *EntryCatalog) UpdateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	return e.store.UpdateBranch(ctx, repositoryID, branchID, ref)
}

func (e *EntryCatalog) GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error) {
	return e.store.GetBranch(ctx, repositoryID, branchID)
}

func (e *EntryCatalog) GetTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) (*graveler.CommitID, error) {
	return e.store.GetTag(ctx, repositoryID, tagID)
}

func (e *EntryCatalog) CreateTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID, commitID graveler.CommitID) error {
	return e.store.CreateTag(ctx, repositoryID, tagID, commitID)
}

func (e *EntryCatalog) DeleteTag(ctx context.Context, repositoryID graveler.RepositoryID, tagID graveler.TagID) error {
	return e.store.DeleteTag(ctx, repositoryID, tagID)
}

func (e *EntryCatalog) ListTags(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.TagID) (graveler.TagIterator, error) {
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
	return e.store.Log(ctx, repositoryID, commitID)
}

func (e *EntryCatalog) ListBranches(ctx context.Context, repositoryID graveler.RepositoryID) (graveler.BranchIterator, error) {
	return e.store.ListBranches(ctx, repositoryID)
}

func (e *EntryCatalog) DeleteBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	return e.store.DeleteBranch(ctx, repositoryID, branchID)
}

func (e *EntryCatalog) WriteMetaRange(ctx context.Context, repositoryID graveler.RepositoryID, it EntryIterator) (*graveler.MetaRangeID, error) {
	return e.store.WriteMetaRange(ctx, repositoryID, NewEntryToValueIterator(it))
}

func (e *EntryCatalog) Commit(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, committer string, message string, metadata graveler.Metadata) (graveler.CommitID, error) {
	return e.store.Commit(ctx, repositoryID, branchID, committer, message, metadata)
}

func (e *EntryCatalog) GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	return e.store.GetCommit(ctx, repositoryID, commitID)
}

func (e *EntryCatalog) Dereference(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref) (graveler.CommitID, error) {
	return e.store.Dereference(ctx, repositoryID, ref)
}

func (e *EntryCatalog) Reset(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	return e.store.Reset(ctx, repositoryID, branchID)
}

func (e *EntryCatalog) ResetKey(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path) error {
	key := graveler.Key(path)
	return e.store.ResetKey(ctx, repositoryID, branchID, key)
}

func (e *EntryCatalog) ResetPrefix(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, prefix Path) error {
	keyPrefix := graveler.Key(prefix)
	return e.store.ResetPrefix(ctx, repositoryID, branchID, keyPrefix)
}

func (e *EntryCatalog) Revert(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (graveler.CommitID, error) {
	return e.store.Revert(ctx, repositoryID, branchID, ref)
}

func (e *EntryCatalog) Merge(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.Ref, to graveler.BranchID, committer string, message string, metadata graveler.Metadata) (graveler.CommitID, error) {
	return e.store.Merge(ctx, repositoryID, from, to, committer, message, metadata)
}

func (e *EntryCatalog) DiffUncommitted(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (EntryDiffIterator, error) {
	iter, err := e.store.DiffUncommitted(ctx, repositoryID, branchID)
	if err != nil {
		return nil, err
	}
	return NewEntryDiffIterator(iter), nil
}

func (e *EntryCatalog) Diff(ctx context.Context, repositoryID graveler.RepositoryID, left, right graveler.Ref) (EntryDiffIterator, error) {
	iter, err := e.store.Diff(ctx, repositoryID, left, right)
	if err != nil {
		return nil, err
	}
	return NewEntryDiffIterator(iter), nil
}

func (e *EntryCatalog) GetEntry(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, path Path) (*Entry, error) {
	val, err := e.store.Get(ctx, repositoryID, ref, graveler.Key(path))
	if err != nil {
		return nil, err
	}
	return ValueToEntry(val)
}

func (e *EntryCatalog) SetEntry(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path, entry *Entry) error {
	key := graveler.Key(path)
	value, err := EntryToValue(entry)
	if err != nil {
		return err
	}
	return e.store.Set(ctx, repositoryID, branchID, key, *value)
}

func (e *EntryCatalog) DeleteEntry(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path) error {
	key := graveler.Key(path)
	return e.store.Delete(ctx, repositoryID, branchID, key)
}

func (e *EntryCatalog) ListEntries(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, prefix, delimiter Path) (EntryListingIterator, error) {
	iter, err := e.store.List(ctx, repositoryID, ref)
	if err != nil {
		return nil, err
	}
	it := NewValueToEntryIterator(iter)
	return NewEntryListingIterator(it, prefix, delimiter), nil
}
