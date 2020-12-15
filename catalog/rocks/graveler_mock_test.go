package rocks

import (
	"context"
	"strings"

	"github.com/treeverse/lakefs/graveler"
)

type GravelerMock struct {
	KeyValue map[string]*graveler.Value
	Err      error
}

func (g GravelerMock) Get(_ context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, key graveler.Key) (*graveler.Value, error) {
	if g.Err != nil {
		return nil, g.Err
	}
	k := g.buildKey(repositoryID, ref, key)
	v := g.KeyValue[k]
	if v == nil {
		return nil, graveler.ErrNotFound
	}
	return v, nil
}

func (g GravelerMock) buildKey(repositoryID graveler.RepositoryID, ref graveler.Ref, key graveler.Key) string {
	return strings.Join([]string{repositoryID.String(), ref.String(), key.String()}, "/")
}

func (g GravelerMock) Set(_ context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, key graveler.Key, value graveler.Value) error {
	if g.Err != nil {
		return g.Err
	}
	k := g.buildKey(repositoryID, graveler.Ref(branchID.String()), key)
	g.KeyValue[k] = &value
	return nil
}

func (g GravelerMock) Delete(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, key graveler.Key) error {
	panic("implement me")
}

func (g GravelerMock) List(ctx context.Context, repositoryID graveler.RepositoryID, ref graveler.Ref, prefix, from, delimiter graveler.Key) (graveler.ListingIterator, error) {
	panic("implement me")
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

func (g GravelerMock) Revert(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, ref graveler.Ref) (graveler.CommitID, error) {
	panic("implement me")
}

func (g GravelerMock) Merge(ctx context.Context, repositoryID graveler.RepositoryID, from graveler.Ref, to graveler.BranchID) (graveler.CommitID, error) {
	panic("implement me")
}

func (g GravelerMock) DiffUncommitted(ctx context.Context, repositoryID graveler.RepositoryID, branchID graveler.BranchID, from graveler.Key) (graveler.DiffIterator, error) {
	panic("implement me")
}

func (g GravelerMock) Diff(ctx context.Context, repositoryID graveler.RepositoryID, left, right graveler.Ref, from graveler.Key) (graveler.DiffIterator, error) {
	panic("implement me")
}
