package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGetStartPos(t *testing.T) {
	/**
	Here are the possible states for where to start iterating, given a prefix, an "after" and a delimiter:
	delim	prefix	after	expected start position
	0		0		0		""
	0		0		1		at after (when iterating we'll skip the exact match anyway)
	0		1		0		at prefix
	0		1		1		at after (when iterating we'll skip the exact match anyway)
	1		0		0		""
	1		0		1		next common prefix after "after"
	1		1		0		at prefix
	1		1		1		next common prefix after "after"
	*/
	cases := []struct {
		Name      string
		Delimiter string
		Prefix    string
		After     string
		Expected  string
	}{
		{"all_empty", "", "", "", ""},
		{"only_after", "", "", "a/b/", "a/b/"},
		{"only_prefix", "", "a/", "", "a/"},
		{"prefix_and_after", "", "a/", "a/b/", "a/b/"},
		{"only_delimiter", "/", "", "", ""},
		{"delimiter_and_after", "/", "", "a/b/", string(graveler.UpperBoundForPrefix([]byte("a/b/")))},
		{"delimiter_and_prefix", "/", "a/", "", "a/"},
		{"delimiter_prefix_and_after", "/", "a/", "a/b/", string(graveler.UpperBoundForPrefix([]byte("a/b/")))},
		{"empty_directory", "/", "", "a/b//", string(graveler.UpperBoundForPrefix([]byte("a/b//")))},
		{"after_before_prefix", "", "c", "a", "c"},
		{"after_before_prefix_with_delim", "/", "c/", "a", "c/"},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			got := GetStartPos(cas.Prefix, cas.After, cas.Delimiter)
			if got != cas.Expected {
				t.Fatalf("expected %s got %s", cas.Expected, got)
			}
		})
	}
}

func TestCatalog_ListRepositories(t *testing.T) {
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
		want        []*Repository
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all",
			args: args{
				limit: -1,
				after: "",
			},
			want: []*Repository{
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
			want: []*Repository{
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
			want: []*Repository{
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
			want: []*Repository{
				{Name: "repo2", StorageNamespace: "storage2", DefaultBranch: "main2", CreationDate: now},
				{Name: "repo3", StorageNamespace: "storage3", DefaultBranch: "main3", CreationDate: now},
			},
			wantHasMore: false,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup Catalog
			gravelerMock := &FakeGraveler{
				RepositoryIteratorFactory: NewFakeRepositoryIteratorFactory(gravelerData),
			}
			c := &Catalog{
				Store: gravelerMock,
			}
			// test method
			ctx := context.Background()
			got, hasMore, err := c.ListRepositories(ctx, tt.args.limit, "", tt.args.after)
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

func TestCatalog_BranchExists(t *testing.T) {
	// prepare branch data
	gravelerData := []*graveler.BranchRecord{
		{BranchID: "branch1", Branch: &graveler.Branch{CommitID: "commit1"}},
		{BranchID: "branch2", Branch: &graveler.Branch{CommitID: "commit2"}},
		{BranchID: "branch3", Branch: &graveler.Branch{CommitID: "commit3"}},
	}
	tests := []struct {
		Branch      string
		ShouldExist bool
	}{{"branch1", true}, {"branch2", true}, {"branch-foo", false}}
	for _, tt := range tests {
		t.Run(tt.Branch, func(t *testing.T) {
			// setup Catalog
			gravelerMock := &FakeGraveler{
				BranchIteratorFactory: testutil.NewFakeBranchIteratorFactory(gravelerData),
			}
			c := &Catalog{
				Store: gravelerMock,
			}
			// test method
			ctx := context.Background()
			exists, err := c.BranchExists(ctx, "repo", tt.Branch)
			if err != nil {
				t.Fatal("BranchExists failed:", err)
			}
			if exists != tt.ShouldExist {
				not := ""
				if !tt.ShouldExist {
					not = " not"
				}
				t.Errorf("branch %s should%s exist", tt.Branch, not)
			}
		})
	}
}

func TestCatalog_ListBranches(t *testing.T) {
	// prepare branch data
	gravelerData := []*graveler.BranchRecord{
		{BranchID: "branch1", Branch: &graveler.Branch{CommitID: "commit1"}},
		{BranchID: "branch2", Branch: &graveler.Branch{CommitID: "commit2"}},
		{BranchID: "branch3", Branch: &graveler.Branch{CommitID: "commit3"}},
	}
	type args struct {
		prefix string
		limit  int
		after  string
	}
	tests := []struct {
		name        string
		args        args
		want        []*Branch
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all",
			args: args{limit: -1},
			want: []*Branch{
				{Name: "branch1", Reference: "commit1"},
				{Name: "branch2", Reference: "commit2"},
				{Name: "branch3", Reference: "commit3"},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "exact",
			args: args{limit: 3},
			want: []*Branch{
				{Name: "branch1", Reference: "commit1"},
				{Name: "branch2", Reference: "commit2"},
				{Name: "branch3", Reference: "commit3"},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "first",
			args: args{limit: 1},
			want: []*Branch{
				{Name: "branch1", Reference: "commit1"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second",
			args: args{limit: 1, after: "branch1"},
			want: []*Branch{
				{Name: "branch2", Reference: "commit2"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last2",
			args: args{limit: 10, after: "branch1"},
			want: []*Branch{
				{Name: "branch2", Reference: "commit2"},
				{Name: "branch3", Reference: "commit3"},
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
			// setup Catalog
			gravelerMock := &FakeGraveler{
				BranchIteratorFactory: testutil.NewFakeBranchIteratorFactory(gravelerData),
			}
			c := &Catalog{
				Store: gravelerMock,
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

func TestCatalog_ListTags(t *testing.T) {
	gravelerData := []*graveler.TagRecord{
		{TagID: "t1", CommitID: "c1"},
		{TagID: "t2", CommitID: "c2"},
		{TagID: "t3", CommitID: "c3"},
	}
	type args struct {
		limit int
		after string
	}
	tests := []struct {
		name        string
		args        args
		want        []*Tag
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all",
			args: args{limit: -1},
			want: []*Tag{
				{ID: "t1", CommitID: "c1"},
				{ID: "t2", CommitID: "c2"},
				{ID: "t3", CommitID: "c3"},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "exact",
			args: args{limit: 3},
			want: []*Tag{
				{ID: "t1", CommitID: "c1"},
				{ID: "t2", CommitID: "c2"},
				{ID: "t3", CommitID: "c3"},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "first",
			args: args{limit: 1},
			want: []*Tag{
				{ID: "t1", CommitID: "c1"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second",
			args: args{limit: 1, after: "t1"},
			want: []*Tag{
				{ID: "t2", CommitID: "c2"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last2",
			args: args{limit: 10, after: "t1"},
			want: []*Tag{
				{ID: "t2", CommitID: "c2"},
				{ID: "t3", CommitID: "c3"},
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
			gravelerMock := &FakeGraveler{
				TagIteratorFactory: NewFakeTagIteratorFactory(gravelerData),
			}
			c := &Catalog{
				Store: gravelerMock,
			}
			ctx := context.Background()
			got, hasMore, err := c.ListTags(ctx, "repo", tt.args.limit, tt.args.after)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ListTags() error = %v, wantErr %v", err, tt.wantErr)
			}
			if diff := deep.Equal(got, tt.want); diff != nil {
				t.Error("ListTags() found diff", diff)
			}
			if hasMore != tt.wantHasMore {
				t.Errorf("ListTags() hasMore = %t, want %t", hasMore, tt.wantHasMore)
			}
		})
	}
}

func TestCatalog_ListEntries(t *testing.T) {
	// prepare branch data
	now := time.Now()
	gravelerData := []*graveler.ValueRecord{
		{Key: graveler.Key("file1"), Value: MustEntryToValue(&Entry{Address: "file1", LastModified: timestamppb.New(now), Size: 1, ETag: "01"})},
		{Key: graveler.Key("file2"), Value: MustEntryToValue(&Entry{Address: "file2", LastModified: timestamppb.New(now), Size: 2, ETag: "02"})},
		{Key: graveler.Key("file3"), Value: MustEntryToValue(&Entry{Address: "file3", LastModified: timestamppb.New(now), Size: 3, ETag: "03"})},
		{Key: graveler.Key("h/file1"), Value: MustEntryToValue(&Entry{Address: "h/file1", LastModified: timestamppb.New(now), Size: 1, ETag: "01"})},
		{Key: graveler.Key("h/file2"), Value: MustEntryToValue(&Entry{Address: "h/file2", LastModified: timestamppb.New(now), Size: 2, ETag: "02"})},
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
		want        []*DBEntry
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all no delimiter",
			args: args{limit: -1},
			want: []*DBEntry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01", ContentType: "application/octet-stream"},
				{Path: "file2", PhysicalAddress: "file2", CreationDate: now, Size: 2, Checksum: "02", ContentType: "application/octet-stream"},
				{Path: "file3", PhysicalAddress: "file3", CreationDate: now, Size: 3, Checksum: "03", ContentType: "application/octet-stream"},
				{Path: "h/file1", PhysicalAddress: "h/file1", CreationDate: now, Size: 1, Checksum: "01", ContentType: "application/octet-stream"},
				{Path: "h/file2", PhysicalAddress: "h/file2", CreationDate: now, Size: 2, Checksum: "02", ContentType: "application/octet-stream"},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "first no delimiter",
			args: args{limit: 1},
			want: []*DBEntry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01", ContentType: "application/octet-stream"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second no delimiter",
			args: args{limit: 1, after: "file1"},
			want: []*DBEntry{
				{Path: "file2", PhysicalAddress: "file2", CreationDate: now, Size: 2, Checksum: "02", ContentType: "application/octet-stream"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last two no delimiter",
			args: args{limit: 10, after: "file3"},
			want: []*DBEntry{
				{Path: "h/file1", PhysicalAddress: "h/file1", CreationDate: now, Size: 1, Checksum: "01", ContentType: "application/octet-stream"},
				{Path: "h/file2", PhysicalAddress: "h/file2", CreationDate: now, Size: 2, Checksum: "02", ContentType: "application/octet-stream"},
			},
			wantHasMore: false,
			wantErr:     false,
		},

		{
			name: "all with delimiter",
			args: args{limit: -1, delimiter: "/"},
			want: []*DBEntry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01", ContentType: "application/octet-stream"},
				{Path: "file2", PhysicalAddress: "file2", CreationDate: now, Size: 2, Checksum: "02", ContentType: "application/octet-stream"},
				{Path: "file3", PhysicalAddress: "file3", CreationDate: now, Size: 3, Checksum: "03", ContentType: "application/octet-stream"},
				{Path: "h/", CommonLevel: true},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "first with delimiter",
			args: args{limit: 1, delimiter: "/"},
			want: []*DBEntry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01", ContentType: "application/octet-stream"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last with delimiter",
			args: args{limit: 1, after: "file3", delimiter: "/"},
			want: []*DBEntry{
				{Path: "h/", CommonLevel: true},
			},
			wantHasMore: false,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup Catalog
			gravelerMock := &FakeGraveler{
				ListIteratorFactory: NewFakeValueIteratorFactory(gravelerData),
			}
			c := &Catalog{
				Store: gravelerMock,
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
