package graveler_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
)

const defaultBranchID = graveler.BranchID("master")

type committedMock struct {
	Values       map[string]*graveler.Value
	diffIterator graveler.DiffIterator
	err          error
	treeID       graveler.TreeID
}

func (c *committedMock) Get(_ context.Context, _ graveler.StorageNamespace, _ graveler.TreeID, key graveler.Key) (*graveler.Value, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.Values[string(key)], nil
}

func (c *committedMock) List(_ context.Context, _ graveler.StorageNamespace, _ graveler.TreeID, _ graveler.Key) (graveler.ValueIterator, error) {
	if c.err != nil {
		return nil, c.err
	}
	records := make([]graveler.ValueRecord, 0, len(c.Values))
	for k, v := range c.Values {
		records = append(records, graveler.ValueRecord{Key: []byte(k), Value: v})
	}
	return &mockValueIterator{
		records: records,
	}, nil
}

func (c *committedMock) Diff(_ context.Context, _ graveler.StorageNamespace, _, _ graveler.TreeID, _ graveler.Key) (graveler.DiffIterator, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.diffIterator, nil
}

func (c *committedMock) Merge(_ context.Context, _ graveler.StorageNamespace, _, _, _ graveler.TreeID) (graveler.TreeID, error) {
	if c.err != nil {
		return "", c.err
	}
	return c.treeID, nil
}

func (c *committedMock) Apply(_ context.Context, _ graveler.StorageNamespace, _ graveler.TreeID, _ graveler.ValueIterator) (graveler.TreeID, error) {
	if c.err != nil {
		return "", c.err
	}
	return c.treeID, nil
}

type stagingMock struct {
	err           error
	Value         *graveler.Value
	ValueIterator graveler.ValueIterator
	stagingToken  graveler.StagingToken
}

func (s *stagingMock) DropByPrefix(_ context.Context, _ graveler.StagingToken, _ graveler.Key) error {
	return nil
}

func (s *stagingMock) Drop(_ context.Context, _ graveler.StagingToken) error {
	if s.err != nil {
		return s.err
	}
	return nil
}

func (s *stagingMock) Get(_ context.Context, _ graveler.StagingToken, _ graveler.Key) (*graveler.Value, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.Value, nil
}

func (s *stagingMock) Set(_ context.Context, _ graveler.StagingToken, _ graveler.Key, _ *graveler.Value) error {
	if s.err != nil {
		return s.err
	}
	return nil
}

func (s *stagingMock) DropKey(_ context.Context, _ graveler.StagingToken, _ graveler.Key) error {
	return nil
}

func (s *stagingMock) List(_ context.Context, _ graveler.StagingToken) (graveler.ValueIterator, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.ValueIterator, nil
}

func (s *stagingMock) Snapshot(_ context.Context, _ graveler.StagingToken) (graveler.StagingToken, error) {
	if s.err != nil {
		return "", s.err
	}
	return s.stagingToken, nil
}

func (s *stagingMock) ListSnapshot(_ context.Context, _ graveler.StagingToken, _ graveler.Key) (graveler.ValueIterator, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.ValueIterator, nil
}

type mockRefs struct {
	listRepositoriesRes graveler.RepositoryIterator
	listBranchesRes     graveler.BranchIterator
	commitIter          graveler.CommitIterator
	refType             graveler.ReferenceType
	branch              *graveler.Branch
	branchErr           error
}

func (m *mockRefs) RevParse(_ context.Context, _ graveler.RepositoryID, _ graveler.Ref) (graveler.Reference, error) {
	var branch graveler.BranchID
	if m.refType == graveler.ReferenceTypeBranch {
		branch = defaultBranchID
	}
	return newMockReference(m.refType, branch, ""), nil
}

func (m *mockRefs) GetRepository(_ context.Context, _ graveler.RepositoryID) (*graveler.Repository, error) {
	return &graveler.Repository{}, nil
}

func (m *mockRefs) CreateRepository(_ context.Context, _ graveler.RepositoryID, _ graveler.Repository, _ graveler.Branch) error {
	return nil
}

func (m *mockRefs) ListRepositories(_ context.Context, _ graveler.RepositoryID) (graveler.RepositoryIterator, error) {
	return m.listRepositoriesRes, nil
}

func (m *mockRefs) DeleteRepository(_ context.Context, _ graveler.RepositoryID) error {
	return nil
}

func (m *mockRefs) GetBranch(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID) (*graveler.Branch, error) {
	return m.branch, m.branchErr
}

func (m *mockRefs) SetBranch(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID, _ graveler.Branch) error {
	return nil
}

func (m *mockRefs) DeleteBranch(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID) error {
	return nil
}

func (m *mockRefs) ListBranches(_ context.Context, _ graveler.RepositoryID, _ graveler.BranchID) (graveler.BranchIterator, error) {
	return m.listBranchesRes, nil
}

func (m *mockRefs) GetCommit(_ context.Context, _ graveler.RepositoryID, _ graveler.CommitID) (*graveler.Commit, error) {
	return &graveler.Commit{}, nil
}

func (m *mockRefs) AddCommit(_ context.Context, _ graveler.RepositoryID, _ graveler.Commit) (graveler.CommitID, error) {
	return "", nil
}

func (m *mockRefs) FindMergeBase(_ context.Context, _ graveler.RepositoryID, _ ...graveler.CommitID) (*graveler.Commit, error) {
	return &graveler.Commit{}, nil
}

func (m *mockRefs) Log(_ context.Context, _ graveler.RepositoryID, _ graveler.CommitID) (graveler.CommitIterator, error) {
	return m.commitIter, nil
}

type ListingIter struct {
	current  int
	listings []graveler.Listing
	err      error
}

func newListingIter(listings []graveler.Listing) *ListingIter {
	return &ListingIter{listings: listings, current: -1}
}

func (r *ListingIter) Next() bool {
	r.current++
	return r.current < len(r.listings)
}

func (r *ListingIter) SeekGE(id graveler.Key) {
	for i, listing := range r.listings {
		if bytes.Compare(id, listing.Key) >= 0 {
			r.current = i - 1
		}
	}
	r.current = len(r.listings)
}

func (r *ListingIter) Value() *graveler.Listing {
	if r.current < 0 || r.current >= len(r.listings) {
		return nil
	}
	return &r.listings[r.current]
}

func (r *ListingIter) Err() error {
	return r.err
}

func (r *ListingIter) Close() {
	return
}

type diffIter struct {
	current int
	records []graveler.Diff
	err     error
}

func newDiffIter(records []graveler.Diff) *diffIter {
	return &diffIter{records: records, current: -1}
}
func (r *diffIter) Next() bool {
	r.current++
	return r.current < len(r.records)
}

func (r *diffIter) SeekGE(id graveler.Key) {
	for i, record := range r.records {
		if bytes.Compare(id, record.Key) >= 0 {
			r.current = i - 1
		}
	}
	r.current = len(r.records)
}

func (r *diffIter) Value() *graveler.Diff {
	if r.current < 0 || r.current >= len(r.records) {
		return nil
	}
	return &r.records[r.current]
}

func (r *diffIter) Err() error {
	return r.err
}

func (r *diffIter) Close() {
	return
}

type mockValueIterator struct {
	current int
	records []graveler.ValueRecord
	err     error
}

func newMockValueIterator(records []graveler.ValueRecord) graveler.ValueIterator {
	return &mockValueIterator{records: records, current: -1}
}

func (r *mockValueIterator) Next() bool {
	r.current++
	return r.current < len(r.records)
}

func (r *mockValueIterator) SeekGE(id graveler.Key) {
	for i, record := range r.records {
		if bytes.Compare(record.Key, id) >= 0 {
			r.current = i - 1
			return
		}
	}
	r.current = len(r.records)
}

func (r *mockValueIterator) Value() *graveler.ValueRecord {
	if r.current < 0 || r.current >= len(r.records) {
		return nil
	}
	return &r.records[r.current]
}

func (r *mockValueIterator) Err() error {
	return r.err
}

func (r *mockValueIterator) Close() {
	return
}

type mockReference struct {
	refType  graveler.ReferenceType
	branch   graveler.Branch
	commitId graveler.CommitID
}

// newMockReference returns a mockReference
// if branch parameter is empty branch record will be nil
func newMockReference(refType graveler.ReferenceType, branchID graveler.BranchID, commitId graveler.CommitID) *mockReference {
	var branch graveler.Branch
	if branchID != "" {
		branch = graveler.Branch{CommitID: commitId}

	}
	return &mockReference{
		refType:  refType,
		branch:   branch,
		commitId: commitId,
	}
}

func (m *mockReference) Type() graveler.ReferenceType {
	return m.refType
}

func (m *mockReference) Branch() graveler.Branch {
	return m.branch
}

func (m *mockReference) CommitID() graveler.CommitID {
	return m.commitId
}

func compareListingIterators(t *testing.T, got, expected graveler.ListingIterator) {
	t.Helper()
	for got.Next() {
		if !expected.Next() {
			t.Fatalf("got next returned true where expected next returned false")
		}
		if diff := deep.Equal(got.Value(), expected.Value()); diff != nil {
			t.Errorf("unexpected diff %s", diff)
		}
	}
	if expected.Next() {
		t.Fatalf("expected next returned true where got next returned false")
	}
}
