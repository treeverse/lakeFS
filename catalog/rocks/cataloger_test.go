package rocks

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/graveler"
)

func Test_cataloger_ListRepositories(t *testing.T) {
	// prepare data tests
	now := time.Now()
	gravelerData := []*graveler.RepositoryRecord{
		{RepositoryID: "repo1", Repository: &graveler.Repository{StorageNamespace: "storage1", CreationDate: now, DefaultBranchID: "main1"}},
		{RepositoryID: "repo2", Repository: &graveler.Repository{StorageNamespace: "storage2", CreationDate: now, DefaultBranchID: "main2"}},
		{RepositoryID: "repo3", Repository: &graveler.Repository{StorageNamespace: "storage3", CreationDate: now, DefaultBranchID: "main3"}},
	}
	type args struct {
		limit int
		after string
	}
	tests := []struct {
		name        string
		args        args
		want        []*catalog.Repository
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all",
			args: args{
				limit: -1,
				after: "",
			},
			want: []*catalog.Repository{
				{Name: "repo1", StorageNamespace: "storage1", DefaultBranch: "main1", CreationDate: now},
				{Name: "repo2", StorageNamespace: "storage2", DefaultBranch: "main2", CreationDate: now},
				{Name: "repo3", StorageNamespace: "storage3", DefaultBranch: "main3", CreationDate: now},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "first",
			args: args{
				limit: 1,
				after: "",
			},
			want: []*catalog.Repository{
				{Name: "repo1", StorageNamespace: "storage1", DefaultBranch: "main1", CreationDate: now},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second",
			args: args{
				limit: 1,
				after: "repo1",
			},
			want: []*catalog.Repository{
				{Name: "repo2", StorageNamespace: "storage2", DefaultBranch: "main2", CreationDate: now},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last2",
			args: args{
				limit: 10,
				after: "repo1",
			},
			want: []*catalog.Repository{
				{Name: "repo2", StorageNamespace: "storage2", DefaultBranch: "main2", CreationDate: now},
				{Name: "repo3", StorageNamespace: "storage3", DefaultBranch: "main3", CreationDate: now},
			},
			wantHasMore: false,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup cataloger
			gravelerMock := &FakeGraveler{
				RepositoryIterator: NewFakeRepositoryIterator(gravelerData),
			}
			c := &cataloger{
				EntryCatalog: &EntryCatalog{
					store: gravelerMock,
				},
			}
			// test method
			ctx := context.Background()
			got, hasMore, err := c.ListRepositories(ctx, tt.args.limit, tt.args.after)
			if tt.wantErr && err == nil {
				t.Fatal("ListRepositories err nil, expected error")
			}
			if err != nil {
				return
			}
			if hasMore != tt.wantHasMore {
				t.Errorf("ListRepositories hasMore %t, expected %t", hasMore, tt.wantHasMore)
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Error("ListRepositories diff found:", diff)
			}
		})
	}
}

func TestCataloger_ListBranches(t *testing.T) {
	// prepare branch data
	gravelerData := []*graveler.BranchRecord{
		{BranchID: "branch1", Branch: &graveler.Branch{CommitID: "commit1"}},
		{BranchID: "branch2", Branch: &graveler.Branch{CommitID: "commit2"}},
		{BranchID: "branch3", Branch: &graveler.Branch{CommitID: "commit3"}},
	}
	type args struct {
		repository graveler.RepositoryID
		prefix     string
		limit      int
		after      string
	}
	tests := []struct {
		name        string
		args        args
		want        []*catalog.Branch
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all",
			args: args{limit: -1},
			want: []*catalog.Branch{
				{Repository: "repo", Name: "branch1"},
				{Repository: "repo", Name: "branch2"},
				{Repository: "repo", Name: "branch3"},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "first",
			args: args{limit: 1},
			want: []*catalog.Branch{
				{Repository: "repo", Name: "branch1"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second",
			args: args{limit: 1, after: "branch1"},
			want: []*catalog.Branch{
				{Repository: "repo", Name: "branch2"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last2",
			args: args{limit: 10, after: "branch1"},
			want: []*catalog.Branch{
				{Repository: "repo", Name: "branch2"},
				{Repository: "repo", Name: "branch3"},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name:        "not found",
			args:        args{limit: 10, after: "zzz"},
			wantHasMore: false,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup cataloger
			gravelerMock := &FakeGraveler{
				BranchIterator: NewFakeBranchIterator(gravelerData),
			}
			c := &cataloger{
				EntryCatalog: &EntryCatalog{
					store: gravelerMock,
				},
			}
			// test method
			ctx := context.Background()
			got, hasMore, err := c.ListBranches(ctx, "repo", tt.args.prefix, tt.args.limit, tt.args.after)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListBranches() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Error("ListBranches() found diff", diff)
			}
			if hasMore != tt.wantHasMore {
				t.Errorf("ListBranches() hasMore = %t, want %t", hasMore, tt.wantHasMore)
			}
		})
	}
}

func TestCataloger_ListEntries(t *testing.T) {
	// prepare branch data
	now := time.Now()
	gravelerData := []*graveler.ValueRecord{
		{Key: graveler.Key("file1"), Value: MustEntryToValue(&Entry{Address: "file1", LastModified: timestamppb.New(now), Size: 1, ETag: []byte{1}})},
		{Key: graveler.Key("file2"), Value: MustEntryToValue(&Entry{Address: "file2", LastModified: timestamppb.New(now), Size: 2, ETag: []byte{2}})},
		{Key: graveler.Key("file3"), Value: MustEntryToValue(&Entry{Address: "file3", LastModified: timestamppb.New(now), Size: 3, ETag: []byte{3}})},
		{Key: graveler.Key("h/file1"), Value: MustEntryToValue(&Entry{Address: "h/file1", LastModified: timestamppb.New(now), Size: 1, ETag: []byte{1}})},
		{Key: graveler.Key("h/file2"), Value: MustEntryToValue(&Entry{Address: "h/file2", LastModified: timestamppb.New(now), Size: 2, ETag: []byte{2}})},
	}
	type args struct {
		prefix    string
		after     string
		delimiter string
		limit     int
	}
	tests := []struct {
		name        string
		args        args
		want        []*catalog.Entry
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all no delimiter",
			args: args{limit: -1},
			want: []*catalog.Entry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01"},
				{Path: "file2", PhysicalAddress: "file2", CreationDate: now, Size: 2, Checksum: "02"},
				{Path: "file3", PhysicalAddress: "file3", CreationDate: now, Size: 3, Checksum: "03"},
				{Path: "h/file1", PhysicalAddress: "h/file1", CreationDate: now, Size: 1, Checksum: "01"},
				{Path: "h/file2", PhysicalAddress: "h/file2", CreationDate: now, Size: 2, Checksum: "02"},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "first no delimiter",
			args: args{limit: 1},
			want: []*catalog.Entry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second no delimiter",
			args: args{limit: 1, after: "file1"},
			want: []*catalog.Entry{
				{Path: "file2", PhysicalAddress: "file2", CreationDate: now, Size: 2, Checksum: "02"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last two no delimiter",
			args: args{limit: 10, after: "file3"},
			want: []*catalog.Entry{
				{Path: "h/file1", PhysicalAddress: "h/file1", CreationDate: now, Size: 1, Checksum: "01"},
				{Path: "h/file2", PhysicalAddress: "h/file2", CreationDate: now, Size: 2, Checksum: "02"},
			},
			wantHasMore: false,
			wantErr:     false,
		},

		{
			name: "all with delimiter",
			args: args{limit: -1, delimiter: "/"},
			want: []*catalog.Entry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01"},
				{Path: "file2", PhysicalAddress: "file2", CreationDate: now, Size: 2, Checksum: "02"},
				{Path: "file3", PhysicalAddress: "file3", CreationDate: now, Size: 3, Checksum: "03"},
				{Path: "h/", CommonLevel: true},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "first with delimiter",
			args: args{limit: 1, delimiter: "/"},
			want: []*catalog.Entry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last with delimiter",
			args: args{limit: 1, after: "file3", delimiter: "/"},
			want: []*catalog.Entry{
				{Path: "h/", CommonLevel: true},
			},
			wantHasMore: false,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup cataloger
			gravelerMock := &FakeGraveler{
				ListIterator: NewFakeValueIterator(gravelerData),
			}
			c := &cataloger{
				EntryCatalog: &EntryCatalog{
					store: gravelerMock,
				},
			}
			// test method
			ctx := context.Background()
			got, hasMore, err := c.ListEntries(ctx, "repo", "ref", tt.args.prefix, tt.args.after, tt.args.delimiter, tt.args.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListEntries() error = %v, wantErr %v", err, tt.wantErr)
			}

			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Error("ListEntries() diff found", diff)
			}
			if hasMore != tt.wantHasMore {
				t.Errorf("ListEntries() hasMore = %t, want %t", hasMore, tt.wantHasMore)
			}
		})
	}
}
