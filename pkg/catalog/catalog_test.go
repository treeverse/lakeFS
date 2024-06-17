package catalog_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	cUtils "github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/graveler"
	gUtils "github.com/treeverse/lakefs/pkg/graveler/testutil"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
	"google.golang.org/protobuf/proto"
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
			got := catalog.GetStartPos(cas.Prefix, cas.After, cas.Delimiter)
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
			// setup Catalog
			gravelerMock := &catalog.FakeGraveler{
				RepositoryIteratorFactory: catalog.NewFakeRepositoryIteratorFactory(gravelerData),
			}
			c := &catalog.Catalog{
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
			gravelerMock := &catalog.FakeGraveler{
				BranchIteratorFactory: gUtils.NewFakeBranchIteratorFactory(gravelerData),
			}
			c := &catalog.Catalog{
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
		want        []*catalog.Branch
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all",
			args: args{limit: -1},
			want: []*catalog.Branch{
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
			want: []*catalog.Branch{
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
			want: []*catalog.Branch{
				{Name: "branch1", Reference: "commit1"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second",
			args: args{limit: 1, after: "branch1"},
			want: []*catalog.Branch{
				{Name: "branch2", Reference: "commit2"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last2",
			args: args{limit: 10, after: "branch1"},
			want: []*catalog.Branch{
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
			gravelerMock := &catalog.FakeGraveler{
				BranchIteratorFactory: gUtils.NewFakeBranchIteratorFactory(gravelerData),
			}
			c := &catalog.Catalog{
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
		want        []*catalog.Tag
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all",
			args: args{limit: -1},
			want: []*catalog.Tag{
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
			want: []*catalog.Tag{
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
			want: []*catalog.Tag{
				{ID: "t1", CommitID: "c1"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second",
			args: args{limit: 1, after: "t1"},
			want: []*catalog.Tag{
				{ID: "t2", CommitID: "c2"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last2",
			args: args{limit: 10, after: "t1"},
			want: []*catalog.Tag{
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
			gravelerMock := &catalog.FakeGraveler{
				TagIteratorFactory: catalog.NewFakeTagIteratorFactory(gravelerData),
			}
			c := &catalog.Catalog{
				Store: gravelerMock,
			}
			ctx := context.Background()
			got, hasMore, err := c.ListTags(ctx, "repo", "", tt.args.limit, tt.args.after)
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
		{Key: graveler.Key("file1"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "file1", LastModified: timestamppb.New(now), Size: 1, ETag: "01"})},
		{Key: graveler.Key("file2"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "file2", LastModified: timestamppb.New(now), Size: 2, ETag: "02"})},
		{Key: graveler.Key("file3"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "file3", LastModified: timestamppb.New(now), Size: 3, ETag: "03"})},
		{Key: graveler.Key("h/file1"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "h/file1", LastModified: timestamppb.New(now), Size: 1, ETag: "01"})},
		{Key: graveler.Key("h/file2"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "h/file2", LastModified: timestamppb.New(now), Size: 2, ETag: "02"})},
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
		want        []*catalog.DBEntry
		wantHasMore bool
		wantErr     bool
	}{
		{
			name: "all no delimiter",
			args: args{limit: -1},
			want: []*catalog.DBEntry{
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
			want: []*catalog.DBEntry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01", ContentType: "application/octet-stream"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "second no delimiter",
			args: args{limit: 1, after: "file1"},
			want: []*catalog.DBEntry{
				{Path: "file2", PhysicalAddress: "file2", CreationDate: now, Size: 2, Checksum: "02", ContentType: "application/octet-stream"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last two no delimiter",
			args: args{limit: 10, after: "file3"},
			want: []*catalog.DBEntry{
				{Path: "h/file1", PhysicalAddress: "h/file1", CreationDate: now, Size: 1, Checksum: "01", ContentType: "application/octet-stream"},
				{Path: "h/file2", PhysicalAddress: "h/file2", CreationDate: now, Size: 2, Checksum: "02", ContentType: "application/octet-stream"},
			},
			wantHasMore: false,
			wantErr:     false,
		},

		{
			name: "all with delimiter",
			args: args{limit: -1, delimiter: "/"},
			want: []*catalog.DBEntry{
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
			want: []*catalog.DBEntry{
				{Path: "file1", PhysicalAddress: "file1", CreationDate: now, Size: 1, Checksum: "01", ContentType: "application/octet-stream"},
			},
			wantHasMore: true,
			wantErr:     false,
		},
		{
			name: "last with delimiter",
			args: args{limit: 1, after: "file3", delimiter: "/"},
			want: []*catalog.DBEntry{
				{Path: "h/", CommonLevel: true},
			},
			wantHasMore: false,
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// setup Catalog
			gravelerMock := &catalog.FakeGraveler{
				ListIteratorFactory: catalog.NewFakeValueIteratorFactory(gravelerData),
			}
			c := &catalog.Catalog{
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

func TestCatalog_PrepareGCUncommitted(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name                   string
		numBranch              int
		numRecords             int
		expectedCalls          int
		expectedForUncommitted int
		compactBranch          bool
	}{
		{
			name:          "no branches",
			numBranch:     0,
			numRecords:    0,
			expectedCalls: 1,
		},
		{
			name:          "no objects",
			numBranch:     3,
			numRecords:    0,
			expectedCalls: 1,
		},
		{
			name:          "sanity",
			numBranch:     5,
			numRecords:    3,
			expectedCalls: 1,
		},
		{
			name:          "tokenized",
			numBranch:     500,
			numRecords:    500,
			expectedCalls: 2,
		},
	}
	for _, tt := range tests {
		for _, compactBranch := range []bool{false, true} {
			t.Run(tt.name, func(t *testing.T) {
				const repositoryID = "repo1"
				g, expectedRecords := createPrepareUncommittedTestScenario(t, repositoryID, tt.numBranch, tt.numRecords, tt.expectedCalls, compactBranch)
				blockAdapter := testutil.NewBlockAdapterByType(t, block.BlockstoreTypeMem)
				c := &catalog.Catalog{
					Store:                 g.Sut,
					BlockAdapter:          blockAdapter,
					UGCPrepareMaxFileSize: 500 * 1024,
					KVStore:               g.KVStore,
				}

				var (
					mark       *catalog.GCUncommittedMark
					runID      string
					allRecords []string
				)
				for {
					result, err := c.PrepareGCUncommitted(ctx, repositoryID, mark)
					require.NoError(t, err)

					// keep or check run id match previous calls
					if runID == "" {
						runID = result.RunID
					} else {
						require.Equal(t, runID, result.RunID)
					}

					if tt.numRecords == 0 {
						require.Equal(t, "", result.Location)
						require.Equal(t, "", result.Filename)
					} else {
						// read parquet information if data was stored to location
						objLocation, err := url.JoinPath(result.Location, result.Filename)
						require.NoError(t, err)
						addresses := readPhysicalAddressesFromParquetObject(t, repositoryID, ctx, c, objLocation)
						allRecords = append(allRecords, addresses...)
					}

					mark = result.Mark
					if mark == nil {
						break
					}
					require.Equal(t, runID, result.Mark.RunID)
				}

				// match expected records found in parquet data
				sort.Strings(allRecords)
				if diff := deep.Equal(allRecords, expectedRecords); diff != nil {
					t.Errorf("Found diff in expected records: %s", diff)
				}
			})
		}
	}
}

func createPrepareUncommittedTestScenario(t *testing.T, repositoryID string, numBranches, numRecords, expectedCalls int, compact bool) (*gUtils.GravelerTest, []string) {
	t.Helper()

	test := gUtils.InitGravelerTest(t)
	records := make([][]*graveler.ValueRecord, numBranches)
	diffs := make([][]graveler.Diff, numBranches)
	var branches []*graveler.BranchRecord
	var expectedRecords []string
	if numBranches > 0 {
		test.RefManager.EXPECT().GetCommit(gomock.Any(), gomock.Any(), gomock.Any()).MinTimes(1).Return(&graveler.Commit{}, nil)
		test.CommittedManager.EXPECT().List(gomock.Any(), gomock.Any(), gomock.Any()).MinTimes(1).Return(cUtils.NewFakeValueIterator([]*graveler.ValueRecord{}), nil)
	}
	for i := 0; i < numBranches; i++ {
		branchID := graveler.BranchID(fmt.Sprintf("branch%04d", i))
		token := graveler.StagingToken(fmt.Sprintf("%s_st%04d", branchID, i))
		branch := &graveler.BranchRecord{BranchID: branchID, Branch: &graveler.Branch{StagingToken: token}}
		compactBaseMetaRangeID := graveler.MetaRangeID(fmt.Sprintf("base%04d", i))
		commitID := graveler.CommitID(fmt.Sprintf("commit%04d", i))
		if !compact {
			test.RefManager.EXPECT().GetBranch(gomock.Any(), gomock.Any(), branchID).MinTimes(1).Return(&graveler.Branch{StagingToken: token}, nil)
		} else {
			branch.CompactedBaseMetaRangeID = compactBaseMetaRangeID
			branch.Branch.CommitID = commitID
			test.RefManager.EXPECT().GetBranch(gomock.Any(), gomock.Any(), branchID).MinTimes(1).Return(&graveler.Branch{StagingToken: token, CommitID: commitID, CompactedBaseMetaRangeID: compactBaseMetaRangeID}, nil)
		}
		branches = append(branches, branch)

		records[i] = make([]*graveler.ValueRecord, 0, numRecords)
		diffs[i] = make([]graveler.Diff, 0, numRecords)
		for j := 0; j < numRecords; j++ {
			var (
				addressType catalog.Entry_AddressType
				address     string
			)
			// create full and relative addresses
			if j%2 == 0 {
				addressType = catalog.Entry_FULL
				address = fmt.Sprintf("%s/%s_record%04d", "mem://"+repositoryID, branchID, j)
			} else {
				addressType = catalog.Entry_RELATIVE
				address = fmt.Sprintf("%s_record%04d", branchID, j)
			}
			e := catalog.Entry{
				Address:      address,
				LastModified: timestamppb.New(time.Now()),
				Size:         0,
				ETag:         "",
				Metadata:     nil,
				AddressType:  addressType,
				ContentType:  "",
			}

			v, err := proto.Marshal(&e)
			require.NoError(t, err)
			records[i] = append(records[i], &graveler.ValueRecord{
				Key: []byte(e.Address),
				Value: &graveler.Value{
					Identity: []byte("dont care"),
					Data:     v,
				},
			})
			// we always keep the relative path - as the prepared uncommitted will trim the storage namespace
			expectedRecords = append(expectedRecords, fmt.Sprintf("%s_record%04d", branchID, j))

			if compact {
				diffs[i] = append(diffs[i], graveler.Diff{
					Type: graveler.DiffTypeAdded,
					Key:  []byte(e.Address),
					Value: &graveler.Value{
						Identity: []byte("dont care"),
						Data:     v,
					},
				}) // record in compaction and in staging so no need to add it again to expected records
				e.Address = fmt.Sprintf("%s_%s", e.Address, "compacted")
				v, err = proto.Marshal(&e)
				require.NoError(t, err)
				diffs[i] = append(diffs[i], graveler.Diff{
					Type: graveler.DiffTypeAdded,
					Key:  []byte(e.Address),
					Value: &graveler.Value{
						Identity: []byte("dont care"),
						Data:     v,
					},
				})
				// record in compaction but not in staging
				expectedRecords = append(expectedRecords, fmt.Sprintf("%s_record%04d_%s", branchID, j, "compacted"))
			}
		}

		// Add tombstone
		records[i] = append(records[i], &graveler.ValueRecord{
			Key:   []byte(fmt.Sprintf("%s_tombstone", branchID)),
			Value: nil,
		})

		diffs[i] = append(diffs[i], graveler.Diff{
			Type: graveler.DiffTypeRemoved,
			Key:  []byte(fmt.Sprintf("%s_tombstone", branchID)),
		})

		// Add external address
		e := catalog.Entry{
			Address:      fmt.Sprintf("external/address/object_%s", branchID),
			LastModified: timestamppb.New(time.Now()),
			Size:         0,
			ETag:         "",
			Metadata:     nil,
			AddressType:  catalog.Entry_FULL,
			ContentType:  "",
		}
		v, err := proto.Marshal(&e)
		require.NoError(t, err)
		records[i] = append(records[i], &graveler.ValueRecord{
			Key: []byte(e.Address),
			Value: &graveler.Value{
				Identity: []byte("dont care"),
				Data:     v,
			},
		})

		diffs[i] = append(diffs[i], graveler.Diff{
			Type: graveler.DiffTypeAdded,
			Key:  []byte(e.Address),
			Value: &graveler.Value{
				Identity: []byte("dont care"),
				Data:     v,
			},
		})
	}

	test.GarbageCollectionManager.EXPECT().NewID().Return("TestRunID")
	repository := &graveler.RepositoryRecord{
		RepositoryID: graveler.RepositoryID(repositoryID),
		Repository: &graveler.Repository{
			StorageNamespace: graveler.StorageNamespace("mem://" + repositoryID),
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		},
	}
	test.RefManager.EXPECT().GetRepository(gomock.Any(), graveler.RepositoryID(repositoryID)).MinTimes(1).Return(repository, nil)

	// expect tracked addresses does not list branches, so remove one and keep at least the first
	test.RefManager.EXPECT().ListBranches(gomock.Any(), gomock.Any()).MinTimes(1).Return(gUtils.NewFakeBranchIterator(branches), nil)
	for i := 0; i < len(branches); i++ {
		sort.Slice(records[i], func(ii, jj int) bool {
			return bytes.Compare(records[i][ii].Key, records[i][jj].Key) < 0
		})
		test.StagingManager.EXPECT().List(gomock.Any(), branches[i].StagingToken, gomock.Any()).MinTimes(1).Return(cUtils.NewFakeValueIterator(records[i]))
		if compact {
			sort.Slice(diffs[i], func(ii, jj int) bool {
				return bytes.Compare(diffs[i][ii].Key, diffs[i][jj].Key) < 0
			})
			test.CommittedManager.EXPECT().Diff(gomock.Any(), repository.StorageNamespace, gomock.Any(), branches[i].CompactedBaseMetaRangeID).MinTimes(1).Return(gUtils.NewDiffIter(diffs[i]), nil)
		}
	}

	if numRecords > 0 {
		test.GarbageCollectionManager.EXPECT().
			GetUncommittedLocation(gomock.Any(), gomock.Any()).
			MinTimes(1).
			DoAndReturn(func(runID string, sn graveler.StorageNamespace) (string, error) {
				return fmt.Sprintf("%s/retention/gc/uncommitted/%s/uncommitted/", "_lakefs", runID), nil
			})
	}

	sort.Strings(expectedRecords)
	return test, expectedRecords
}

func readPhysicalAddressesFromParquetObject(t *testing.T, repositoryID string, ctx context.Context, c *catalog.Catalog, obj string) []string {
	objReader, err := c.BlockAdapter.Get(ctx, block.ObjectPointer{
		Identifier:       obj,
		IdentifierType:   block.IdentifierTypeFull,
		StorageNamespace: "mem://" + repositoryID,
	})
	require.NoError(t, err)
	defer func() { _ = objReader.Close() }()

	data, err := io.ReadAll(objReader)
	require.NoError(t, err)
	bufferFile := buffer.NewBufferFileFromBytes(data)
	defer func() { _ = bufferFile.Close() }()
	pr, err := reader.NewParquetReader(bufferFile, new(catalog.UncommittedParquetObject), 4)
	require.NoError(t, err)

	count := pr.GetNumRows()
	u := make([]*catalog.UncommittedParquetObject, count)
	err = pr.Read(&u)
	require.NoError(t, err)
	pr.ReadStop()

	var records []string
	for _, record := range u {
		records = append(records, record.PhysicalAddress)
	}
	return records
}
