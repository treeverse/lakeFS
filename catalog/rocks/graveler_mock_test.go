package rocks

import (
	"context"
	"strings"

	"github.com/treeverse/lakefs/graveler"
)

type GravelerMock struct {
	KeyValue     map[string]*graveler.Value
	Err          error
	ListIterator graveler.ListingIterator
	DiffIterator graveler.DiffIterator
}

func (g GravelerMock) formatKey(repositoryID graveler.RepositoryID, ref graveler.Ref, key graveler.Key) string {
	return strings.Join([]string{repositoryID.String(), ref.String(), key.String()}, "/")
}

func (g GravelerMock) Get(_ context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, key graveler.Key) (*graveler.Value, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	k := g.formatKey(repositoryID, ref, key)
	v := g.KeyValue[k]
	if v == nil {
		return nil, graveler.ErrNotFound
	}
	return v, nil
}

func (g GravelerMock) Set(_ context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, key graveler.Key, value graveler.Value) error {
	if g.Err != nil {
		return g.Err
	}
	k := g.formatKey(repositoryID, graveler.Ref(branchID.String()), key)
	g.KeyValue[k] = &value
	return nil
}

func (g GravelerMock) Delete(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, key graveler.Key) error {
	panic("implement me")
}

func (g GravelerMock) List(_ context.Context, _ graveler.RepositoryID, _ graveler.Ref, _, _, _ graveler.Key) (graveler.ListingIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.ListIterator, nil
}

func (g GravelerMock) GetRepository(ctx context.Context, repositoryID graveler.RepositoryID) (*graveler.Repository, error) {
	panic("implement me")
}

func (g GravelerMock) CreateRepository(ctx context.Context, repositoryID graveler.RepositoryID, storageNamespace graveler.StorageNamespace, branchID graveler.BranchID) (*graveler.Repository, error) {
	panic("implement me")
}

func (g GravelerMock) ListRepositories(ctx context.Context, from graveler.RepositoryID) (graveler.RepositoryIterator, error) {
	panic("implement me")
}

func (g GravelerMock) DeleteRepository(ctx context.Context, repositoryID graveler.RepositoryID) error {
	panic("implement me")
}

func (g GravelerMock) CreateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	panic("implement me")
}

func (g GravelerMock) UpdateBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (*graveler.Branch, error) {
	panic("implement me")
}

func (g GravelerMock) GetBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) (*graveler.Branch, error) {
	panic("implement me")
}

func (g GravelerMock) Log(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (graveler.CommitIterator, error) {
	panic("implement me")
}

func (g GravelerMock) ListBranches(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.BranchID) (graveler.BranchIterator, error) {
	panic("implement me")
}

func (g GravelerMock) DeleteBranch(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	panic("implement me")
}

func (g GravelerMock) Commit(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, committer string, message string, metadata graveler.Metadata) (graveler.CommitID, error) {
	panic("implement me")
}

func (g GravelerMock) GetCommit(ctx context.Context, repositoryID graveler.RepositoryID, commitID graveler.CommitID) (*graveler.Commit, error) {
	panic("implement me")
}

func (g GravelerMock) Dereference(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref) (graveler.CommitID, error) {
	panic("implement me")
}

func (g GravelerMock) Reset(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID) error {
	panic("implement me")
}

func (g GravelerMock) ResetKey(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, key graveler.Key) error {
	panic("implement me")
}

func (g GravelerMock) ResetPrefix(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, key graveler.Key) error {
	panic("implement me")
}

func (g GravelerMock) Revert(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (graveler.CommitID, error) {
	panic("implement me")
}

func (g GravelerMock) Merge(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.Ref, to graveler.BranchID) (graveler.CommitID, error) {
	panic("implement me")
}

func (g GravelerMock) DiffUncommitted(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, from graveler.Key) (graveler.DiffIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.DiffIterator, nil
}

func (g GravelerMock) Diff(_ context.Context, _ graveler.RepositoryID, _, _ graveler.Ref, _ graveler.Key) (graveler.DiffIterator, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	return g.DiffIterator, nil
}

type MockListingIterator struct {
	Data  []*graveler.Listing
	Index int
}

func NewMockListingIterator(data []*graveler.Listing) *MockListingIterator {
	return &MockListingIterator{Data: data, Index: -1}
}

func (m *MockListingIterator) Next() bool {
	if m.Index >= len(m.Data) {
		return false
	}
	m.Index++
	return m.Index < len(m.Data)
}

func (m *MockListingIterator) SeekGE(_ graveler.Key) {
	panic("implement me")
}

func (m *MockListingIterator) Value() *graveler.Listing {
	return m.Data[m.Index]
}

func (m *MockListingIterator) Err() error {
	return nil
}

func (m *MockListingIterator) Close() {}

type MockDiffIterator struct {
	Data  []*graveler.Diff
	Index int
}

func NewMockDiffIterator(data []*graveler.Diff) *MockDiffIterator {
	return &MockDiffIterator{Data: data, Index: -1}
}

func (m *MockDiffIterator) Next() bool {
	if m.Index >= len(m.Data) {
		return false
	}
	m.Index++
	return m.Index < len(m.Data)
}

func (m *MockDiffIterator) SeekGE(_ graveler.Key) {
	panic("implement me")
}

func (m *MockDiffIterator) Value() *graveler.Diff {
	return m.Data[m.Index]
}

func (m *MockDiffIterator) Err() error {
	return nil
}

func (m *MockDiffIterator) Close() {}
