package graveler_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/graveler"

	"github.com/go-test/deep"
)

type committedMock struct {
	Value         *graveler.Value
	ValueIterator graveler.ValueIterator
	diffIterator  graveler.DiffIterator
	err           error
	treeID        graveler.TreeID
}

func (c *committedMock) Get(_ context.Context, _ graveler.StorageNamespace, _ graveler.TreeID, _ graveler.Key) (*graveler.Value, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.Value, nil
}

func (c *committedMock) List(_ context.Context, _ graveler.StorageNamespace, _ graveler.TreeID, _ graveler.Key) (graveler.ValueIterator, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.ValueIterator, nil
}

func (c *committedMock) Diff(_ context.Context, _ graveler.StorageNamespace, _, _, _ graveler.TreeID, _ graveler.Key) (graveler.DiffIterator, error) {
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

func (s *stagingMock) Set(_ context.Context, _ graveler.StagingToken, _ graveler.Key, _ graveler.Value) error {
	if s.err != nil {
		return s.err
	}
	return nil
}

func (s *stagingMock) Delete(_ context.Context, _ graveler.StagingToken, _ graveler.Key) error {
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
}

const defaultBranchName = graveler.BranchID("master")

func (m *mockRefs) RevParse(_ context.Context, _ graveler.RepositoryID, _ graveler.Ref) (graveler.Reference, error) {
	var branch graveler.BranchID
	if m.refType == graveler.ReferenceTypeBranch {
		branch = defaultBranchName
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
	return &graveler.Branch{}, nil
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

type ValueIter struct {
	current int
	records []graveler.ValueRecord
	err     error
}

func newValueIter(records []graveler.ValueRecord) *ValueIter {
	return &ValueIter{records: records, current: -1}
}
func (r *ValueIter) Next() bool {
	r.current++
	return r.current < len(r.records)
}

func (r *ValueIter) SeekGE(id graveler.Key) {
	for i, record := range r.records {
		if bytes.Compare(record.Key, id) >= 0 {
			r.current = i - 1
			return
		}
	}
	r.current = len(r.records)
}

func (r *ValueIter) Value() *graveler.ValueRecord {
	if r.current < 0 || r.current >= len(r.records) {
		return nil
	}
	return &r.records[r.current]
}

func (r *ValueIter) Err() error {
	return r.err
}

func (r *ValueIter) Close() {
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

func compareListingIterators(got, expected graveler.ListingIterator, t *testing.T) {
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

func TestGravel_prefixIterator(t *testing.T) {
	tests := []struct {
		name               string
		valueIter          graveler.ValueIterator
		prefix             []byte
		seekTo             []byte
		expectedPrefixIter graveler.ValueIterator
	}{
		{
			name:               "no prefix",
			valueIter:          newValueIter([]graveler.ValueRecord{{Key: []byte("foo")}}),
			expectedPrefixIter: newValueIter([]graveler.ValueRecord{{Key: []byte("foo")}}),
		},
		{
			name:               "no files ",
			valueIter:          newValueIter([]graveler.ValueRecord{{Key: []byte("other/path/foo")}}),
			prefix:             []byte("path/"),
			expectedPrefixIter: newValueIter([]graveler.ValueRecord{}),
		},
		{
			name:               "one file ",
			valueIter:          newValueIter([]graveler.ValueRecord{{Key: []byte("path/foo")}}),
			prefix:             []byte("path/"),
			expectedPrefixIter: newValueIter([]graveler.ValueRecord{{Key: []byte("path/foo"), Value: nil}}),
		},
		{
			name:               "one file in prefix  ",
			prefix:             []byte("path/"),
			valueIter:          newValueIter([]graveler.ValueRecord{{Key: []byte("before/foo")}, {Key: []byte("path/foo")}, {Key: []byte("last/foo")}}),
			expectedPrefixIter: newValueIter([]graveler.ValueRecord{{Key: []byte("path/foo"), Value: nil}}),
		},
		{
			name:               "seek before",
			prefix:             []byte("path/"),
			valueIter:          newValueIter([]graveler.ValueRecord{{Key: []byte("before/foo")}, {Key: []byte("path/foo")}, {Key: []byte("last/foo")}}),
			seekTo:             []byte("before/"),
			expectedPrefixIter: newValueIter([]graveler.ValueRecord{{Key: []byte("path/foo"), Value: nil}}),
		},
		{
			name:               "seek after",
			prefix:             []byte("path/"),
			valueIter:          newValueIter([]graveler.ValueRecord{{Key: []byte("before/foo")}, {Key: []byte("path/foo")}, {Key: []byte("z_after/foo")}}),
			seekTo:             []byte("z_after/"),
			expectedPrefixIter: newValueIter([]graveler.ValueRecord{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefixIter := graveler.NewPrefixIterator(tt.valueIter, tt.prefix)
			prefixIter.SeekGE(tt.seekTo)
			// compare iterators
			for prefixIter.Next() {
				if !tt.expectedPrefixIter.Next() {
					t.Fatalf("listing next returned true where expected listing next returned false")
				}
				if diff := deep.Equal(prefixIter.Value(), tt.expectedPrefixIter.Value()); diff != nil {
					t.Errorf("unexpected diff %s", diff)
				}
			}
			if tt.expectedPrefixIter.Next() {
				t.Fatalf("expected listing next returned true where listing next returned false")
			}

		})
	}
}

func TestGravel_listingIterator(t *testing.T) {
	tests := []struct {
		name                string
		valueIter           graveler.ValueIterator
		delimiter           []byte
		prefix              []byte
		expectedListingIter graveler.ListingIterator
	}{
		{
			name:                "no file",
			valueIter:           newValueIter([]graveler.ValueRecord{}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{}),
		},
		{
			name:                "one file no delimiter",
			valueIter:           newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo")}}),
			delimiter:           nil,
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: false, Key: graveler.Key("foo")}}),
		},
		{
			name:                "one file",
			valueIter:           newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo")}}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: false, Key: graveler.Key("foo")}}),
		},
		{
			name:                "one common prefix",
			valueIter:           newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo/bar")}, {Key: graveler.Key("foo/bar2")}}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("foo")}}),
		},
		{
			name:                "one common prefix one file",
			valueIter:           newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo/bar")}, {Key: graveler.Key("foo/bar2")}, {Key: graveler.Key("foo/bar3")}, {Key: graveler.Key("foo/bar4")}, {Key: graveler.Key("fooFighter")}}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("foo")}, {CommonPrefix: false, Key: graveler.Key("fooFighter")}}),
		},
		{
			name:                "one file with prefix",
			valueIter:           newValueIter([]graveler.ValueRecord{{Key: graveler.Key("path/to/foo")}}),
			delimiter:           []byte("/"),
			prefix:              []byte("path/to/"),
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: false, Key: graveler.Key("path/to/foo")}}),
		},
		{
			name:                "one common prefix",
			valueIter:           newValueIter([]graveler.ValueRecord{{Key: graveler.Key("path/to/foo/bar")}, {Key: graveler.Key("path/to/foo/bar2")}}),
			delimiter:           []byte("/"),
			prefix:              []byte("path/to/"),
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("path/to/foo")}}),
		},
		{
			name:                "one common prefix one file",
			valueIter:           newValueIter([]graveler.ValueRecord{{Key: graveler.Key("path/to/foo/bar")}, {Key: graveler.Key("path/to/foo/bar2")}, {Key: graveler.Key("path/to/foo/bar3")}, {Key: graveler.Key("path/to/foo/bar4")}, {Key: graveler.Key("path/to/fooFighter")}}),
			delimiter:           []byte("/"),
			prefix:              []byte("path/to/"),
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("path/to/foo")}, {CommonPrefix: false, Key: graveler.Key("path/to/fooFighter")}}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listingIter := graveler.NewListingIter(tt.valueIter, tt.delimiter, tt.prefix)
			compareListingIterators(listingIter, tt.expectedListingIter, t)
		})
	}
}

func TestGravel_List(t *testing.T) {
	tests := []struct {
		name            string
		r               graveler.Gravel
		amount          int
		delimiter       graveler.Key
		from            graveler.Key
		prefix          graveler.Key
		expectedErr     error
		expectedHasMore bool
		expectedListing graveler.ListingIterator
	}{
		{
			name: "one committed one staged no paths",
			r: graveler.Gravel{
				CommittedManager: &committedMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{}}})},
				StagingManager:   &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("bar"), Value: &graveler.Value{}}})},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeBranch},
			},
			delimiter:       graveler.Key("/"),
			prefix:          graveler.Key(""),
			amount:          10,
			expectedListing: newListingIter([]graveler.Listing{{Key: graveler.Key("bar"), Value: &graveler.Value{}}, {Key: graveler.Key("foo"), Value: &graveler.Value{}}}),
		},
		{
			name: "same path different file",
			r: graveler.Gravel{
				CommittedManager: &committedMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("original")}}})},
				StagingManager:   &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}})},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeBranch},
			},
			delimiter:       graveler.Key("/"),
			prefix:          graveler.Key(""),
			amount:          10,
			expectedListing: newListingIter([]graveler.Listing{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}}),
		},
		{
			name: "one committed one staged no paths - with prefix",
			r: graveler.Gravel{
				CommittedManager: &committedMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("prefix/foo"), Value: &graveler.Value{}}})},
				StagingManager:   &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("prefix/bar"), Value: &graveler.Value{}}})},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeBranch},
			},
			delimiter:       graveler.Key("/"),
			prefix:          graveler.Key("prefix/"),
			amount:          10,
			expectedListing: newListingIter([]graveler.Listing{{Key: graveler.Key("prefix/bar"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/foo"), Value: &graveler.Value{}}}),
		},
		{
			name: "objects and paths in both committed and staging",
			r: graveler.Gravel{
				CommittedManager: &committedMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("prefix/pathA/foo"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathA/foo2"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathB/foo"), Value: &graveler.Value{}}})},
				StagingManager:   &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("prefix/file"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathA/bar"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathB/bar"), Value: &graveler.Value{}}})},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeBranch},
			},
			delimiter: graveler.Key("/"),
			prefix:    graveler.Key("prefix/"),
			amount:    10,
			expectedListing: newListingIter([]graveler.Listing{
				{
					CommonPrefix: false,
					Key:          graveler.Key("prefix/file"),
					Value:        &graveler.Value{},
				}, {
					CommonPrefix: true,
					Key:          graveler.Key("prefix/pathA"),
					Value:        nil,
				}, {
					CommonPrefix: true,
					Key:          graveler.Key("prefix/pathB"),
					Value:        nil,
				}}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listing, err := tt.r.List(context.Background(), "", "", tt.prefix, tt.from, tt.delimiter)
			if err != tt.expectedErr {
				t.Fatalf("wrong error, expected:%s got:%s ", tt.expectedErr, err)
			}
			if err != nil {
				return // err == tt.expectedErr
			}
			// compare iterators
			compareListingIterators(listing, tt.expectedListing, t)
		})
	}
}

func TestGravel_Get(t *testing.T) {
	var ErrTest = errors.New("some kind of err")
	tests := []struct {
		name                string
		r                   graveler.Gravel
		expectedValueResult graveler.Value
		expectedErr         error
	}{
		{
			name: "commit - exists",
			r: graveler.Gravel{
				CommittedManager: &committedMock{Value: &graveler.Value{Identity: []byte("committed")}},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeCommit},
			},

			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "commit - not found",
			r: graveler.Gravel{
				CommittedManager: &committedMock{err: graveler.ErrNotFound},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeCommit},
			},
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "commit - error",
			r: graveler.Gravel{
				CommittedManager: &committedMock{err: ErrTest},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeCommit},
			},
			expectedErr: ErrTest,
		},
		{
			name: "branch - only staged",
			r: graveler.Gravel{
				StagingManager:   &stagingMock{Value: &graveler.Value{Identity: []byte("staged")}},
				CommittedManager: &committedMock{err: graveler.ErrNotFound},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeBranch},
			},
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch - committed and staged",
			r: graveler.Gravel{
				StagingManager:   &stagingMock{Value: &graveler.Value{Identity: []byte("staged")}},
				CommittedManager: &committedMock{Value: &graveler.Value{Identity: []byte("committed")}},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeBranch},
			},
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch -  only committed",
			r: graveler.Gravel{
				StagingManager:   &stagingMock{err: graveler.ErrNotFound},
				CommittedManager: &committedMock{Value: &graveler.Value{Identity: []byte("committed")}},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeBranch},
			},
			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "branch -  tombstone",
			r: graveler.Gravel{
				StagingManager:   &stagingMock{Value: nil},
				CommittedManager: &committedMock{Value: &graveler.Value{Identity: []byte("committed")}},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeBranch},
			},
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "branch -  staged return error",
			r: graveler.Gravel{
				StagingManager:   &stagingMock{err: ErrTest},
				CommittedManager: &committedMock{},
				RefManager:       &mockRefs{refType: graveler.ReferenceTypeBranch},
			},
			expectedErr: ErrTest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Value, err := tt.r.Get(context.Background(), "", "", nil)
			if err != tt.expectedErr {
				t.Fatalf("wrong error, expected:%s got:%s ", tt.expectedErr, err)
			}
			if err != nil {
				return // err == tt.expected error
			}
			if string(tt.expectedValueResult.Identity) != string(Value.Identity) {
				t.Errorf("wrong Value address, expected:%s got:%s ", tt.expectedValueResult.Identity, Value.Identity)
			}
		})
	}
}

func TestGravel_Merge(t *testing.T) {

}

func TestGravel_Reset(t *testing.T) {

}

func TestGravel_Revert(t *testing.T) {

}

func TestGravel_DiffUncommitted(t *testing.T) {
	tests := []struct {
		name            string
		r               graveler.Gravel
		amount          int
		expectedErr     error
		expectedHasMore bool
		expectedDiff    graveler.DiffIterator
	}{
		{
			name: "no changes",
			r: graveler.Gravel{
				CommittedManager: &committedMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}}), err: graveler.ErrNotFound},
				StagingManager:   &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{})},
				RefManager:       &mockRefs{},
			},
			amount:       10,
			expectedDiff: newDiffIter([]graveler.Diff{}),
		},
		{
			name: "added one",
			r: graveler.Gravel{
				CommittedManager: &committedMock{ValueIterator: newValueIter([]graveler.ValueRecord{}), err: graveler.ErrNotFound},
				StagingManager:   &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				RefManager:       &mockRefs{},
			},
			amount: 10,
			expectedDiff: newDiffIter([]graveler.Diff{{
				Key:   graveler.Key("foo/one"),
				Type:  graveler.DiffTypeAdded,
				Value: &graveler.Value{},
			}}),
		},
		{
			name: "changed one",
			r: graveler.Gravel{
				CommittedManager: &committedMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				StagingManager:   &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				RefManager:       &mockRefs{},
			},
			amount: 10,
			expectedDiff: newDiffIter([]graveler.Diff{{
				Key:   graveler.Key("foo/one"),
				Type:  graveler.DiffTypeChanged,
				Value: &graveler.Value{},
			}}),
		},
		{
			name: "removed one",
			r: graveler.Gravel{
				CommittedManager: &committedMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				StagingManager:   &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: nil}})},
				RefManager:       &mockRefs{},
			},
			amount: 10,
			expectedDiff: newDiffIter([]graveler.Diff{{
				Key:  graveler.Key("foo/one"),
				Type: graveler.DiffTypeRemoved,
			}}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diff, err := tt.r.DiffUncommitted(context.Background(), "repo", "branch", graveler.Key("from"))
			if err != tt.expectedErr {
				t.Fatalf("wrong error, expected:%s got:%s ", tt.expectedErr, err)
			}
			if err != nil {
				return // err == tt.expectedErr
			}

			// compare iterators
			for diff.Next() {
				if !tt.expectedDiff.Next() {
					t.Fatalf("listing next returned true where expected listing next returned false")
				}
				if diff := deep.Equal(diff.Value(), tt.expectedDiff.Value()); diff != nil {
					t.Errorf("unexpected diff %s", diff)
				}
			}
			if tt.expectedDiff.Next() {
				t.Fatalf("expected listing next returned true where listing next returned false")
			}
		})
	}
}

func TestGravel_UpdateBranch(t *testing.T) {
	gravel := graveler.Gravel{
		CommittedManager: nil,
		StagingManager:   &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
		RefManager:       &mockRefs{},
	}
	_, err := gravel.UpdateBranch(context.Background(), "", "", "")
	if !errors.Is(err, graveler.ErrConflictFound) {
		t.Fatal("expected update to fail on conflict")
	}

	gravel.StagingManager = &stagingMock{ValueIterator: newValueIter([]graveler.ValueRecord{})}
	_, err = gravel.UpdateBranch(context.Background(), "", "", "")
	if err != nil {
		t.Fatal("did not expect to get error")
	}
}
