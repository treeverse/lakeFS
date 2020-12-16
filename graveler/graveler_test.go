package graveler_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
)

func TestGraveler_PrefixIterator(t *testing.T) {
	tests := []struct {
		name               string
		valueIter          graveler.ValueIterator
		prefix             []byte
		seekTo             []byte
		expectedPrefixIter graveler.ValueIterator
	}{
		{
			name:               "no prefix",
			valueIter:          newMockValueIterator([]graveler.ValueRecord{{Key: []byte("foo")}}),
			expectedPrefixIter: newMockValueIterator([]graveler.ValueRecord{{Key: []byte("foo")}}),
		},
		{
			name:               "no files",
			valueIter:          newMockValueIterator([]graveler.ValueRecord{{Key: []byte("other/path/foo")}}),
			prefix:             []byte("path/"),
			expectedPrefixIter: newMockValueIterator([]graveler.ValueRecord{}),
		},
		{
			name:               "one file",
			valueIter:          newMockValueIterator([]graveler.ValueRecord{{Key: []byte("path/foo")}}),
			prefix:             []byte("path/"),
			expectedPrefixIter: newMockValueIterator([]graveler.ValueRecord{{Key: []byte("path/foo"), Value: nil}}),
		},
		{
			name:               "one file in prefix",
			prefix:             []byte("path/"),
			valueIter:          newMockValueIterator([]graveler.ValueRecord{{Key: []byte("before/foo")}, {Key: []byte("path/foo")}, {Key: []byte("last/foo")}}),
			expectedPrefixIter: newMockValueIterator([]graveler.ValueRecord{{Key: []byte("path/foo"), Value: nil}}),
		},
		{
			name:               "seek before",
			prefix:             []byte("path/"),
			valueIter:          newMockValueIterator([]graveler.ValueRecord{{Key: []byte("before/foo")}, {Key: []byte("path/foo")}, {Key: []byte("last/foo")}}),
			seekTo:             []byte("before/"),
			expectedPrefixIter: newMockValueIterator([]graveler.ValueRecord{{Key: []byte("path/foo"), Value: nil}}),
		},
		{
			name:               "seek after",
			prefix:             []byte("path/"),
			valueIter:          newMockValueIterator([]graveler.ValueRecord{{Key: []byte("before/foo")}, {Key: []byte("path/foo")}, {Key: []byte("z_after/foo")}}),
			seekTo:             []byte("z_after/"),
			expectedPrefixIter: newMockValueIterator([]graveler.ValueRecord{}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefixIter := graveler.NewPrefixIterator(tt.valueIter, tt.prefix)
			defer prefixIter.Close()
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

func TestGraveler_ListingIterator(t *testing.T) {
	tests := []struct {
		name                string
		valueIter           graveler.ValueIterator
		delimiter           []byte
		prefix              []byte
		expectedListingIter graveler.ListingIterator
	}{
		{
			name:                "no file",
			valueIter:           newMockValueIterator([]graveler.ValueRecord{}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{}),
		},
		{
			name:                "one file no delimiter",
			valueIter:           newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo")}}),
			delimiter:           nil,
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: false, Key: graveler.Key("foo")}}),
		},
		{
			name:                "one file",
			valueIter:           newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo")}}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: false, Key: graveler.Key("foo")}}),
		},
		{
			name:                "one common prefix",
			valueIter:           newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo/bar")}, {Key: graveler.Key("foo/bar2")}}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("foo/")}}),
		},
		{
			name:                "one common prefix one file",
			valueIter:           newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo/bar")}, {Key: graveler.Key("foo/bar2")}, {Key: graveler.Key("foo/bar3")}, {Key: graveler.Key("foo/bar4")}, {Key: graveler.Key("fooFighter")}}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("foo/")}, {CommonPrefix: false, Key: graveler.Key("fooFighter")}}),
		},
		{
			name:                "one file with prefix",
			valueIter:           newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("path/to/foo")}}),
			delimiter:           []byte("/"),
			prefix:              []byte("path/to/"),
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: false, Key: graveler.Key("path/to/foo")}}),
		},
		{
			name:                "one common prefix with prefix",
			valueIter:           newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("path/to/foo/bar")}, {Key: graveler.Key("path/to/foo/bar2")}}),
			delimiter:           []byte("/"),
			prefix:              []byte("path/to/"),
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("path/to/foo/")}}),
		},
		{
			name:                "one common prefix one file with prefix",
			valueIter:           newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("path/to/foo/bar")}, {Key: graveler.Key("path/to/foo/bar2")}, {Key: graveler.Key("path/to/foo/bar3")}, {Key: graveler.Key("path/to/foo/bar4")}, {Key: graveler.Key("path/to/fooFighter")}}),
			delimiter:           []byte("/"),
			prefix:              []byte("path/to/"),
			expectedListingIter: newListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("path/to/foo/")}, {CommonPrefix: false, Key: graveler.Key("path/to/fooFighter")}}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listingIter := graveler.NewListingIterator(tt.valueIter, tt.delimiter, tt.prefix)
			defer listingIter.Close()
			compareListingIterators(t, listingIter, tt.expectedListingIter)
		})
	}
}

func TestGraveler_List(t *testing.T) {
	tests := []struct {
		name            string
		r               graveler.Graveler
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
			r: graveler.NewGraveler(&committedMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{}}})},
				&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("bar"), Value: &graveler.Value{}}})},
				&mockRefs{refType: graveler.ReferenceTypeBranch},
			),
			delimiter:       graveler.Key("/"),
			prefix:          graveler.Key(""),
			amount:          10,
			expectedListing: newListingIter([]graveler.Listing{{Key: graveler.Key("bar"), Value: &graveler.Value{}}, {Key: graveler.Key("foo"), Value: &graveler.Value{}}}),
		},
		{
			name: "same path different file",
			r: graveler.NewGraveler(&committedMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("original")}}})},
				&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}})},
				&mockRefs{refType: graveler.ReferenceTypeBranch},
			),
			delimiter:       graveler.Key("/"),
			prefix:          graveler.Key(""),
			amount:          10,
			expectedListing: newListingIter([]graveler.Listing{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}}),
		},
		{
			name: "one committed one staged no paths - with prefix",
			r: graveler.NewGraveler(&committedMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("prefix/foo"), Value: &graveler.Value{}}})},
				&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("prefix/bar"), Value: &graveler.Value{}}})},
				&mockRefs{refType: graveler.ReferenceTypeBranch},
			),
			delimiter:       graveler.Key("/"),
			prefix:          graveler.Key("prefix/"),
			amount:          10,
			expectedListing: newListingIter([]graveler.Listing{{Key: graveler.Key("prefix/bar"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/foo"), Value: &graveler.Value{}}}),
		},
		{
			name: "objects and paths in both committed and staging",
			r: graveler.NewGraveler(&committedMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("prefix/pathA/foo"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathA/foo2"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathB/foo"), Value: &graveler.Value{}}})},
				&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("prefix/file"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathA/bar"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathB/bar"), Value: &graveler.Value{}}})},
				&mockRefs{refType: graveler.ReferenceTypeBranch},
			),
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
					Key:          graveler.Key("prefix/pathA/"),
					Value:        nil,
				}, {
					CommonPrefix: true,
					Key:          graveler.Key("prefix/pathB/"),
					Value:        nil,
				}}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listing, err := tt.r.List(context.Background(), "", "", tt.prefix, tt.from, tt.delimiter)
			if err != tt.expectedErr {
				t.Fatalf("wrong error, expected:%s got:%s", tt.expectedErr, err)
			}
			if err != nil {
				return // err == tt.expectedErr
			}
			defer listing.Close()
			// compare iterators
			compareListingIterators(t, listing, tt.expectedListing)
		})
	}
}

func TestGraveler_Get(t *testing.T) {
	var ErrTest = errors.New("some kind of err")
	tests := []struct {
		name                string
		r                   graveler.Graveler
		expectedValueResult graveler.Value
		expectedErr         error
	}{
		{
			name: "commit - exists",
			r: graveler.NewGraveler(&committedMock{Value: &graveler.Value{Identity: []byte("committed")}}, nil,
				&mockRefs{refType: graveler.ReferenceTypeCommit},
			),
			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "commit - not found",
			r: graveler.NewGraveler(&committedMock{err: graveler.ErrNotFound}, nil,
				&mockRefs{refType: graveler.ReferenceTypeCommit},
			), expectedErr: graveler.ErrNotFound,
		},
		{
			name: "commit - error",
			r: graveler.NewGraveler(&committedMock{err: ErrTest}, nil,
				&mockRefs{refType: graveler.ReferenceTypeCommit},
			), expectedErr: ErrTest,
		},
		{
			name: "branch - only staged",
			r: graveler.NewGraveler(&committedMock{err: graveler.ErrNotFound}, &stagingMock{Value: &graveler.Value{Identity: []byte("staged")}},
				&mockRefs{refType: graveler.ReferenceTypeBranch},
			),
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch - committed and staged",
			r: graveler.NewGraveler(&committedMock{Value: &graveler.Value{Identity: []byte("committed")}}, &stagingMock{Value: &graveler.Value{Identity: []byte("staged")}},

				&mockRefs{refType: graveler.ReferenceTypeBranch},
			),
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch - only committed",
			r: graveler.NewGraveler(&committedMock{Value: &graveler.Value{Identity: []byte("committed")}}, &stagingMock{err: graveler.ErrNotFound},

				&mockRefs{refType: graveler.ReferenceTypeBranch},
			),
			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "branch - tombstone",
			r: graveler.NewGraveler(&committedMock{Value: &graveler.Value{Identity: []byte("committed")}}, &stagingMock{Value: nil},

				&mockRefs{refType: graveler.ReferenceTypeBranch},
			),
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "branch - staged return error",
			r: graveler.NewGraveler(&committedMock{}, &stagingMock{err: ErrTest},
				&mockRefs{refType: graveler.ReferenceTypeBranch},
			),
			expectedErr: ErrTest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Value, err := tt.r.Get(context.Background(), "", "", nil)
			if err != tt.expectedErr {
				t.Fatalf("wrong error, expected:%s got:%s", tt.expectedErr, err)
			}
			if err != nil {
				return // err == tt.expected error
			}
			if string(tt.expectedValueResult.Identity) != string(Value.Identity) {
				t.Errorf("wrong Value address, expected:%s got:%s", tt.expectedValueResult.Identity, Value.Identity)
			}
		})
	}
}

func TestGraveler_DiffUncommitted(t *testing.T) {
	tests := []struct {
		name            string
		r               graveler.Graveler
		amount          int
		expectedErr     error
		expectedHasMore bool
		expectedDiff    graveler.DiffIterator
	}{
		{
			name: "no changes",
			r: graveler.NewGraveler(&committedMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}}), err: graveler.ErrNotFound},
				&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{})},
				&mockRefs{branch: &graveler.Branch{}},
			),
			amount:       10,
			expectedDiff: newDiffIter([]graveler.Diff{}),
		},
		{
			name: "added one",
			r: graveler.NewGraveler(&committedMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{}), err: graveler.ErrNotFound},
				&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&mockRefs{branch: &graveler.Branch{}},
			),
			amount: 10,
			expectedDiff: newDiffIter([]graveler.Diff{{
				Key:   graveler.Key("foo/one"),
				Type:  graveler.DiffTypeAdded,
				Value: &graveler.Value{},
			}}),
		},
		{
			name: "changed one",
			r: graveler.NewGraveler(&committedMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&mockRefs{branch: &graveler.Branch{}},
			),
			amount: 10,
			expectedDiff: newDiffIter([]graveler.Diff{{
				Key:   graveler.Key("foo/one"),
				Type:  graveler.DiffTypeChanged,
				Value: &graveler.Value{},
			}}),
		},
		{
			name: "removed one",
			r: graveler.NewGraveler(&committedMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: nil}})},
				&mockRefs{branch: &graveler.Branch{}},
			),
			amount: 10,
			expectedDiff: newDiffIter([]graveler.Diff{{
				Key:  graveler.Key("foo/one"),
				Type: graveler.DiffTypeRemoved,
			}}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diff, err := tt.r.DiffUncommitted(context.Background(), "repo", "branch")
			if err != tt.expectedErr {
				t.Fatalf("wrong error, expected:%s got:%s", tt.expectedErr, err)
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

func TestGraveler_CreateBranch(t *testing.T) {
	gravel := graveler.NewGraveler(nil,
		nil,
		&mockRefs{
			err: graveler.ErrNotFound,
		},
	)
	_, err := gravel.CreateBranch(context.Background(), "", "", "")
	if err != nil {
		t.Fatal("unexpected error on create branch", err)
	}
	// test create branch when branch exists
	gravel = graveler.NewGraveler(nil,
		nil,
		&mockRefs{
			branch: &graveler.Branch{},
		},
	)
	_, err = gravel.CreateBranch(context.Background(), "", "", "")
	if !errors.Is(err, graveler.ErrBranchExists) {
		t.Fatal("did not get expected error, expected ErrBranchExists")
	}
}

func TestGraveler_UpdateBranch(t *testing.T) {
	gravel := graveler.NewGraveler(nil,
		&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
		&mockRefs{branch: &graveler.Branch{}},
	)
	_, err := gravel.UpdateBranch(context.Background(), "", "", "")
	if !errors.Is(err, graveler.ErrConflictFound) {
		t.Fatal("expected update to fail on conflict")
	}
	gravel = graveler.NewGraveler(nil,
		&stagingMock{ValueIterator: newMockValueIterator([]graveler.ValueRecord{})},
		&mockRefs{branch: &graveler.Branch{}},
	)
	_, err = gravel.UpdateBranch(context.Background(), "", "", "")
	if err != nil {
		t.Fatal("did not expect to get error")
	}
}
