package graveler_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/testutil"
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
			valueIter:          testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("foo")}}),
			expectedPrefixIter: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("foo")}}),
		},
		{
			name:               "no files",
			valueIter:          testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("other/path/foo")}}),
			prefix:             []byte("path/"),
			expectedPrefixIter: testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
		},
		{
			name:               "one file",
			valueIter:          testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("path/foo")}}),
			prefix:             []byte("path/"),
			expectedPrefixIter: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("path/foo"), Value: nil}}),
		},
		{
			name:               "one file in prefix",
			prefix:             []byte("path/"),
			valueIter:          testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("before/foo")}, {Key: []byte("path/foo")}, {Key: []byte("last/foo")}}),
			expectedPrefixIter: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("path/foo"), Value: nil}}),
		},
		{
			name:               "seek before",
			prefix:             []byte("path/"),
			valueIter:          testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("before/foo")}, {Key: []byte("path/foo")}, {Key: []byte("last/foo")}}),
			seekTo:             []byte("before/"),
			expectedPrefixIter: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("path/foo"), Value: nil}}),
		},
		{
			name:               "seek after",
			prefix:             []byte("path/"),
			valueIter:          testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: []byte("before/foo")}, {Key: []byte("path/foo")}, {Key: []byte("z_after/foo")}}),
			seekTo:             []byte("z_after/"),
			expectedPrefixIter: testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
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
			valueIter:           testutil.NewValueIteratorFake([]graveler.ValueRecord{}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: testutil.NewListingIter([]graveler.Listing{}),
		},
		{
			name:                "one file no delimiter",
			valueIter:           testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo")}}),
			delimiter:           nil,
			prefix:              nil,
			expectedListingIter: testutil.NewListingIter([]graveler.Listing{{CommonPrefix: false, Key: graveler.Key("foo")}}),
		},
		{
			name:                "one file",
			valueIter:           testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo")}}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: testutil.NewListingIter([]graveler.Listing{{CommonPrefix: false, Key: graveler.Key("foo")}}),
		},
		{
			name:                "one common prefix",
			valueIter:           testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/bar")}, {Key: graveler.Key("foo/bar2")}}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: testutil.NewListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("foo/")}}),
		},
		{
			name:                "one common prefix one file",
			valueIter:           testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/bar")}, {Key: graveler.Key("foo/bar2")}, {Key: graveler.Key("foo/bar3")}, {Key: graveler.Key("foo/bar4")}, {Key: graveler.Key("fooFighter")}}),
			delimiter:           []byte("/"),
			prefix:              nil,
			expectedListingIter: testutil.NewListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("foo/")}, {CommonPrefix: false, Key: graveler.Key("fooFighter")}}),
		},
		{
			name:                "one file with prefix",
			valueIter:           testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("path/to/foo")}}),
			delimiter:           []byte("/"),
			prefix:              []byte("path/to/"),
			expectedListingIter: testutil.NewListingIter([]graveler.Listing{{CommonPrefix: false, Key: graveler.Key("path/to/foo")}}),
		},
		{
			name:                "one common prefix with prefix",
			valueIter:           testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("path/to/foo/bar")}, {Key: graveler.Key("path/to/foo/bar2")}}),
			delimiter:           []byte("/"),
			prefix:              []byte("path/to/"),
			expectedListingIter: testutil.NewListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("path/to/foo/")}}),
		},
		{
			name:                "one common prefix one file with prefix",
			valueIter:           testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("path/to/foo/bar")}, {Key: graveler.Key("path/to/foo/bar2")}, {Key: graveler.Key("path/to/foo/bar3")}, {Key: graveler.Key("path/to/foo/bar4")}, {Key: graveler.Key("path/to/fooFighter")}}),
			delimiter:           []byte("/"),
			prefix:              []byte("path/to/"),
			expectedListingIter: testutil.NewListingIter([]graveler.Listing{{CommonPrefix: true, Key: graveler.Key("path/to/foo/")}, {CommonPrefix: false, Key: graveler.Key("path/to/fooFighter")}}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listingIter := graveler.NewListingIterator(tt.valueIter, tt.delimiter, tt.prefix)
			defer listingIter.Close()
			testutil.CompareListingIterators(t, listingIter, tt.expectedListingIter)
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
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("bar"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			delimiter:       graveler.Key("/"),
			prefix:          graveler.Key(""),
			amount:          10,
			expectedListing: testutil.NewListingIter([]graveler.Listing{{Key: graveler.Key("bar"), Value: &graveler.Value{}}, {Key: graveler.Key("foo"), Value: &graveler.Value{}}}),
		},
		{
			name: "same path different file",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("original")}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			delimiter:       graveler.Key("/"),
			prefix:          graveler.Key(""),
			amount:          10,
			expectedListing: testutil.NewListingIter([]graveler.Listing{{Key: graveler.Key("foo"), Value: &graveler.Value{Identity: []byte("other")}}}),
		},
		{
			name: "one committed one staged no paths - with prefix",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("prefix/foo"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("prefix/bar"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			delimiter:       graveler.Key("/"),
			prefix:          graveler.Key("prefix/"),
			amount:          10,
			expectedListing: testutil.NewListingIter([]graveler.Listing{{Key: graveler.Key("prefix/bar"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/foo"), Value: &graveler.Value{}}}),
		},
		{
			name: "objects and paths in both committed and staging",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("prefix/pathA/foo"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathA/foo2"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathB/foo"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("prefix/file"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathA/bar"), Value: &graveler.Value{}}, {Key: graveler.Key("prefix/pathB/bar"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			delimiter: graveler.Key("/"),
			prefix:    graveler.Key("prefix/"),
			amount:    10,
			expectedListing: testutil.NewListingIter([]graveler.Listing{
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
			testutil.CompareListingIterators(t, listing, tt.expectedListing)
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
			r: graveler.NewGraveler(&testutil.CommittedFake{Value: &graveler.Value{Identity: []byte("committed")}}, nil,
				&testutil.RefsFake{RefType: graveler.ReferenceTypeCommit, Commit: &graveler.Commit{}},
			),
			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "commit - not found",
			r: graveler.NewGraveler(&testutil.CommittedFake{Err: graveler.ErrNotFound}, nil,
				&testutil.RefsFake{RefType: graveler.ReferenceTypeCommit, Commit: &graveler.Commit{}},
			), expectedErr: graveler.ErrNotFound,
		},
		{
			name: "commit - error",
			r: graveler.NewGraveler(&testutil.CommittedFake{Err: ErrTest}, nil,
				&testutil.RefsFake{RefType: graveler.ReferenceTypeCommit, Commit: &graveler.Commit{}},
			), expectedErr: ErrTest,
		},
		{
			name: "branch - only staged",
			r: graveler.NewGraveler(&testutil.CommittedFake{Err: graveler.ErrNotFound}, &testutil.StagingFake{Value: &graveler.Value{Identity: []byte("staged")}},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch - committed and staged",
			r: graveler.NewGraveler(&testutil.CommittedFake{Value: &graveler.Value{Identity: []byte("committed")}}, &testutil.StagingFake{Value: &graveler.Value{Identity: []byte("staged")}},

				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expectedValueResult: graveler.Value{Identity: []byte("staged")},
		},
		{
			name: "branch - only committed",
			r: graveler.NewGraveler(&testutil.CommittedFake{Value: &graveler.Value{Identity: []byte("committed")}}, &testutil.StagingFake{Err: graveler.ErrNotFound},

				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expectedValueResult: graveler.Value{Identity: []byte("committed")},
		},
		{
			name: "branch - tombstone",
			r: graveler.NewGraveler(&testutil.CommittedFake{Value: &graveler.Value{Identity: []byte("committed")}}, &testutil.StagingFake{Value: nil},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
			),
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "branch - staged return error",
			r: graveler.NewGraveler(&testutil.CommittedFake{}, &testutil.StagingFake{Err: ErrTest},
				&testutil.RefsFake{RefType: graveler.ReferenceTypeBranch, Commit: &graveler.Commit{}},
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
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}}), Err: graveler.ErrNotFound},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{})},
				&testutil.RefsFake{Branch: &graveler.Branch{}, Commit: &graveler.Commit{}},
			),
			amount:       10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{}),
		},
		{
			name: "added one",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{}), Err: graveler.ErrNotFound},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{Branch: &graveler.Branch{}, Commit: &graveler.Commit{}},
			),
			amount: 10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{{
				Key:   graveler.Key("foo/one"),
				Type:  graveler.DiffTypeAdded,
				Value: &graveler.Value{},
			}}),
		},
		{
			name: "changed one",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.RefsFake{Branch: &graveler.Branch{}, Commit: &graveler.Commit{}},
			),
			amount: 10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{{
				Key:   graveler.Key("foo/one"),
				Type:  graveler.DiffTypeChanged,
				Value: &graveler.Value{},
			}}),
		},
		{
			name: "removed one",
			r: graveler.NewGraveler(&testutil.CommittedFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
				&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: nil}})},
				&testutil.RefsFake{Branch: &graveler.Branch{}, Commit: &graveler.Commit{}},
			),
			amount: 10,
			expectedDiff: testutil.NewDiffIter([]graveler.Diff{{
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
		&testutil.RefsFake{
			Err: graveler.ErrNotFound,
		},
	)
	_, err := gravel.CreateBranch(context.Background(), "", "", "")
	if err != nil {
		t.Fatal("unexpected error on create branch", err)
	}
	// test create branch when branch exists
	gravel = graveler.NewGraveler(nil,
		nil,
		&testutil.RefsFake{
			Branch: &graveler.Branch{},
		},
	)
	_, err = gravel.CreateBranch(context.Background(), "", "", "")
	if !errors.Is(err, graveler.ErrBranchExists) {
		t.Fatal("did not get expected error, expected ErrBranchExists")
	}
}

func TestGraveler_UpdateBranch(t *testing.T) {
	gravel := graveler.NewGraveler(nil,
		&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: graveler.Key("foo/one"), Value: &graveler.Value{}}})},
		&testutil.RefsFake{Branch: &graveler.Branch{}},
	)
	_, err := gravel.UpdateBranch(context.Background(), "", "", "")
	if !errors.Is(err, graveler.ErrConflictFound) {
		t.Fatal("expected update to fail on conflict")
	}
	gravel = graveler.NewGraveler(nil,
		&testutil.StagingFake{ValueIterator: testutil.NewValueIteratorFake([]graveler.ValueRecord{})},
		&testutil.RefsFake{Branch: &graveler.Branch{}},
	)
	_, err = gravel.UpdateBranch(context.Background(), "", "", "")
	if err != nil {
		t.Fatal("did not expect to get error")
	}
}

func TestGraveler_Commit(t *testing.T) {
	expectedCommitID := graveler.CommitID("expectedCommitId")
	expectedTreeID := graveler.TreeID("expectedTreeID")
	values := testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: nil, Value: nil}})
	type fields struct {
		CommittedManager *testutil.CommittedFake
		StagingManager   *testutil.StagingFake
		RefManager       *testutil.RefsFake
	}
	type args struct {
		ctx          context.Context
		repositoryID graveler.RepositoryID
		branchID     graveler.BranchID
		committer    string
		message      string
		metadata     graveler.Metadata
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        graveler.CommitID
		expectedErr error
	}{
		{
			name: "valid commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{TreeID: expectedTreeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     graveler.Metadata{},
			},
			want:        expectedCommitID,
			expectedErr: nil,
		},
		{
			name: "fail on staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{TreeID: expectedTreeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values, Err: graveler.ErrNotFound},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "fail on apply",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{TreeID: expectedTreeID, Err: graveler.ErrConflictFound},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrConflictFound,
		},
		{
			name: "fail on add commit",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{TreeID: expectedTreeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}, CommitErr: graveler.ErrConflictFound},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     nil,
			},
			want:        expectedCommitID,
			expectedErr: graveler.ErrConflictFound,
		},
		{
			name: "fail on drop",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{TreeID: expectedTreeID},
				StagingManager:   &testutil.StagingFake{ValueIterator: values, DropErr: graveler.ErrNotFound},
				RefManager:       &testutil.RefsFake{CommitID: expectedCommitID, Branch: &graveler.Branch{CommitID: expectedCommitID}},
			},
			args: args{
				ctx:          nil,
				repositoryID: "repo",
				branchID:     "branch",
				committer:    "committer",
				message:      "a message",
				metadata:     graveler.Metadata{},
			},
			want:        expectedCommitID,
			expectedErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectedCommitID := graveler.CommitID("expectedCommitId")
			expectedTreeID := graveler.TreeID("expectedTreeID")
			values := testutil.NewValueIteratorFake([]graveler.ValueRecord{{Key: nil, Value: nil}})

			g := graveler.NewGraveler(tt.fields.CommittedManager, tt.fields.StagingManager, tt.fields.RefManager)
			tt.fields.CommittedManager.TreeID = expectedTreeID
			tt.fields.RefManager.Commit = &graveler.Commit{TreeID: expectedTreeID}
			got, err := g.Commit(context.Background(), "", "", tt.args.committer, tt.args.message, tt.args.metadata)
			if !errors.Is(err, tt.expectedErr) {
				t.Fatalf("unexpected err got = %v, wanted = %v", err, tt.expectedErr)
			}
			if err != nil {
				return
			}
			if diff := deep.Equal(tt.fields.CommittedManager.AppliedData, testutil.AppliedData{
				Values: values,
				TreeID: expectedTreeID,
			}); diff != nil {
				t.Errorf("unexpected apply data %s", diff)
			}

			if diff := deep.Equal(tt.fields.RefManager.AddedCommit, testutil.AddedCommitData{
				Committer: tt.args.committer,
				Message:   tt.args.message,
				TreeID:    expectedTreeID,
				Parents:   graveler.CommitParents{expectedCommitID},
				Metadata:  graveler.Metadata{},
			}); diff != nil {
				t.Errorf("unexpected added commit %s", diff)
			}
			if !tt.fields.StagingManager.DropCalled && tt.fields.StagingManager.DropErr == nil {
				t.Errorf("expected drop to be called")
			}

			if got != expectedCommitID {
				t.Errorf("got wrong commitID, got = %v, want %v", got, expectedCommitID)
			}
		})
	}
}

func TestGraveler_Delete(t *testing.T) {
	type fields struct {
		CommittedManager graveler.CommittedManager
		StagingManager   *testutil.StagingFake
		RefManager       graveler.RefManager
	}
	type args struct {
		ctx          context.Context
		repositoryID graveler.RepositoryID
		branchID     graveler.BranchID
		key          graveler.Key
	}
	tests := []struct {
		name               string
		fields             fields
		args               args
		expectedSetValue   *graveler.ValueRecord
		expectedRemovedKey graveler.Key
		expectedErr        error
	}{
		{
			name: "exists only in committed",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					Value: &graveler.Value{},
				},
				StagingManager: &testutil.StagingFake{
					Err: graveler.ErrNotFound,
				},
				RefManager: &testutil.RefsFake{
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
				},
			},
			args: args{
				key: []byte("key"),
			},
			expectedSetValue: &graveler.ValueRecord{
				Key:   []byte("key"),
				Value: nil,
			},
			expectedErr: nil,
		},
		{
			name: "exists in committed and in staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					Value: &graveler.Value{},
				},
				StagingManager: &testutil.StagingFake{
					Value: &graveler.Value{},
				},
				RefManager: &testutil.RefsFake{
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
				},
			},
			args: args{
				key: []byte("key"),
			},
			expectedSetValue: &graveler.ValueRecord{
				Key:   []byte("key"),
				Value: nil,
			},
			expectedErr: nil,
		},
		{
			name: "exists in committed tombstone in staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					Value: &graveler.Value{},
				},
				StagingManager: &testutil.StagingFake{
					Value: nil,
				},
				RefManager: &testutil.RefsFake{
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
				},
			},
			args:        args{},
			expectedErr: graveler.ErrNotFound,
		},
		{
			name: "exists only in staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					Err: graveler.ErrNotFound,
				},
				StagingManager: &testutil.StagingFake{
					Value: &graveler.Value{},
				},
				RefManager: &testutil.RefsFake{
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
				},
			},
			args: args{
				key: []byte("key"),
			},
			expectedRemovedKey: []byte("key"),
			expectedErr:        nil,
		},
		{
			name: "not in committed not in staging",
			fields: fields{
				CommittedManager: &testutil.CommittedFake{
					Err: graveler.ErrNotFound,
				},
				StagingManager: &testutil.StagingFake{
					Err: graveler.ErrNotFound,
				},
				RefManager: &testutil.RefsFake{
					Branch: &graveler.Branch{},
					Commit: &graveler.Commit{},
				},
			},
			args:        args{},
			expectedErr: graveler.ErrNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := graveler.NewGraveler(tt.fields.CommittedManager, tt.fields.StagingManager, tt.fields.RefManager)
			if err := g.Delete(tt.args.ctx, tt.args.repositoryID, tt.args.branchID, tt.args.key); !errors.Is(err, tt.expectedErr) {
				t.Errorf("Delete() returned unexpected error. got = %v, expected %v", err, tt.expectedErr)
			}
			// validate set on staging
			if diff := deep.Equal(tt.fields.StagingManager.LastSetValueRecord, tt.expectedSetValue); diff != nil {
				t.Errorf("unexpected set value %s", diff)
			}
			// validate removed from staging
			if bytes.Compare(tt.fields.StagingManager.LastRemovedKey, tt.expectedRemovedKey) != 0 {
				t.Errorf("unexpected removed key got = %s, expected = %s ", tt.fields.StagingManager.LastRemovedKey, tt.expectedRemovedKey)
			}

		})
	}
}
