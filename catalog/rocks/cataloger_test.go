package rocks

import (
	"context"
	"testing"
	"time"

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
				EntryCatalog: &entryCatalog{
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
		repository string
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
				{Name: "branch1"},
				{Name: "branch2"},
				{Name: "branch3"},
			},
			wantHasMore: false,
			wantErr:     false,
		},
		{
			name: "first",
			args: args{limit: 1},
			want: []*catalog.Branch{
				{Name: "branch1"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second",
			args: args{limit: 1, after: "branch1"},
			want: []*catalog.Branch{
				{Name: "branch2"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last2",
			args: args{limit: 10, after: "branch1"},
			want: []*catalog.Branch{
				{Name: "branch2"},
				{Name: "branch3"},
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
				EntryCatalog: &entryCatalog{
					store: gravelerMock,
				},
			}
			// test method
			ctx := context.Background()
			got, hasMore, err := c.ListBranches(ctx, tt.args.repository, tt.args.prefix, tt.args.limit, tt.args.after)
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
