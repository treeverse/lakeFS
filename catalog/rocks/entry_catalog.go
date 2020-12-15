//go:generate protoc --proto_path=. --go_out=. --go_opt=paths=source_relative catalog.proto
package rocks

import (
	"context"

	"github.com/treeverse/lakefs/graveler"
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

// EntryCatalog
type EntryCatalog interface {
	// GetRepository returns the Repository metadata object for the given RepositoryID
	GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.Repository, error)

	// CreateRepository stores a new Repository under RepositoryID with the given Branch as default branch
	CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageNamespace graveler.StorageNamespace, branchID graveler.BranchID) (*graveler.Repository, error)

	// ListRepositories returns iterator to scan repositories
	ListRepositories(ctx context.Context, from graveler.RepositoryID) (graveler.RepositoryIterator, error)

	// DeleteRepository deletes the repository
	DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error

	// CreateBranch creates branch on repository pointing to ref
	CreateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error)

	// UpdateBranch updates branch on repository pointing to ref
	UpdateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error)

	// GetBranch gets branch information by branch / repository id
	GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error)

	// Log returns an iterator starting at commit ID up to repository root
	Log(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (graveler.CommitIterator, error)

	// ListBranches lists branches on repositories
	ListBranches(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.BranchID) (graveler.BranchIterator, error)

	// DeleteBranch deletes branch from repository
	DeleteBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error

	// Commit the staged data and returns a commit ID that references that change
	//   ErrNothingToCommit in case there is no data in stage
	Commit(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, committer string, message string, metadata graveler.Metadata) (graveler.CommitID, error)

	// GetCommit returns the Commit metadata object for the given CommitID
	GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error)

	// Dereference returns the commit ID based on 'ref' reference
	Dereference(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref) (graveler.CommitID, error)

	// Reset throw all staged data on the repository / branch
	Reset(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error

	// Revert commits a change that will revert all the changes make from 'ref' specified
	Revert(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (graveler.CommitID, error)

	// Merge merge 'from' with 'to' branches under repository returns the new commit id on 'to' branch
	Merge(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.Ref, to graveler.BranchID) (graveler.CommitID, error)

	// DiffUncommitted returns iterator to scan the changes made on the branch
	DiffUncommitted(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, from graveler.Key) (EntryDiffIterator, error)

	// Diff returns the changes between 'left' and 'right' ref, starting from the 'from' key
	Diff(ctx context.Context, repositoryID graveler.RepositoryID, left, right graveler.Ref, from graveler.Key) (EntryDiffIterator, error)

	// Get returns entry from repository / reference by path, nil entry is a valid entry for tombstone
	// returns error if entry does not exist
	GetEntry(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, path Path) (*Entry, error)

	// Set stores entry on repository / branch by path. nil entry is a valid entry for tombstone
	SetEntry(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path, entry *Entry) error

	// DeleteEntry deletes entry from repository / branch by path
	DeleteEntry(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path) error

	// List lists entries on repository / ref will filter by prefix, from path 'from'.
	//   When 'delimiter' is set the listing will include common prefixes based on the delimiter
	ListEntries(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, prefix, from, delimiter Path) (EntryListingIterator, error)
}

func NewPath(id string) (Path, error) {
	return Path(id), nil
}

func (id Path) String() string {
	return string(id)
}

type entryCatalog struct {
	store graveler.Graveler
}

func NewEntryCatalog() EntryCatalog {
	return &entryCatalog{
		store: graveler.NewGraveler(nil, nil, nil),
	}
}

func (e entryCatalog) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.Repository, error) {
	return e.store.GetRepository(ctx, repositoryID)
}

func (e entryCatalog) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageNamespace graveler.StorageNamespace, branchID graveler.BranchID) (*graveler.Repository, error) {
	return e.store.CreateRepository(ctx, repositoryID, storageNamespace, branchID)
}

func (e entryCatalog) ListRepositories(ctx context.Context, from graveler.RepositoryID) (graveler.RepositoryIterator, error) {
	return e.store.ListRepositories(ctx, from)
}

func (e entryCatalog) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	return e.store.DeleteRepository(ctx, repositoryID)
}

func (e entryCatalog) CreateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	return e.store.CreateBranch(ctx, repositoryID, branchID, ref)
}

func (e entryCatalog) UpdateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	return e.store.UpdateBranch(ctx, repositoryID, branchID, ref)
}

func (e entryCatalog) GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error) {
	return e.store.GetBranch(ctx, repositoryID, branchID)
}

func (e entryCatalog) Log(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (graveler.CommitIterator, error) {
	return e.store.Log(ctx, repositoryID, commitID)
}

func (e entryCatalog) ListBranches(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.BranchID) (graveler.BranchIterator, error) {
	return e.store.ListBranches(ctx, repositoryID, from)
}

func (e entryCatalog) DeleteBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	return e.store.DeleteBranch(ctx, repositoryID, branchID)
}

func (e entryCatalog) Commit(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, committer string, message string, metadata graveler.Metadata) (graveler.CommitID, error) {
	return e.store.Commit(ctx, repositoryID, branchID, committer, message, metadata)
}

func (e entryCatalog) GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	return e.store.GetCommit(ctx, repositoryID, commitID)
}

func (e entryCatalog) Dereference(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref) (graveler.CommitID, error) {
	return e.store.Dereference(ctx, repositoryID, ref)
}

func (e entryCatalog) Reset(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	return e.store.Reset(ctx, repositoryID, branchID)
}

func (e entryCatalog) Revert(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (graveler.CommitID, error) {
	return e.store.Revert(ctx, repositoryID, branchID, ref)
}

func (e entryCatalog) Merge(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.Ref, to graveler.BranchID) (graveler.CommitID, error) {
	return e.store.Merge(ctx, repositoryID, from, to)
}

func (e entryCatalog) DiffUncommitted(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, from graveler.Key) (EntryDiffIterator, error) {
	iter, err := e.store.DiffUncommitted(ctx, repositoryID, branchID, from)
	if err != nil {
		return nil, err
	}
	return NewEntryDiffIterator(iter), nil
}

func (e entryCatalog) Diff(ctx context.Context, repositoryID graveler.RepositoryID, left, right graveler.Ref, from graveler.Key) (EntryDiffIterator, error) {
	iter, err := e.store.Diff(ctx, repositoryID, left, right, from)
	if err != nil {
		return nil, err
	}
	return NewEntryDiffIterator(iter), nil
}

func (e entryCatalog) GetEntry(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, path Path) (*Entry, error) {
	val, err := e.store.Get(ctx, repositoryID, ref, graveler.Key(path))
	if err != nil {
		return nil, err
	}
	return ValueToEntry(val)
}

func (e entryCatalog) SetEntry(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path, entry *Entry) error {
	key, err := graveler.NewKey(path.String())
	if err != nil {
		return err
	}
	value, err := EntryToValue(entry)
	if err != nil {
		return err
	}
	return e.store.Set(ctx, repositoryID, branchID, key, *value)
}

func (e entryCatalog) DeleteEntry(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, path Path) error {
	key, err := graveler.NewKey(path.String())
	if err != nil {
		return err
	}
	return e.store.Delete(ctx, repositoryID, branchID, key)
}

func (e entryCatalog) ListEntries(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, prefix, from, delimiter Path) (EntryListingIterator, error) {
	iter, err := e.store.List(ctx, repositoryID, ref, graveler.Key(prefix), graveler.Key(from), graveler.Key(delimiter))
	if err != nil {
		return nil, err
	}
	return NewEntryListingIterator(iter), nil
}
