package catalog_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	now                      = time.Now()
	uncommittedBranchRecords = []*graveler.ValueRecord{
		{Key: graveler.Key("file1"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "file1", LastModified: timestamppb.New(now), Size: 1, ETag: "01"})},
		{Key: graveler.Key("file2"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "file2", LastModified: timestamppb.New(now), Size: 2, ETag: "02"})},
		{Key: graveler.Key("file3"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "file3", LastModified: timestamppb.New(now), Size: 3, ETag: "03"})},
		{Key: graveler.Key("h/file1"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "h/file1", LastModified: timestamppb.New(now), Size: 1, ETag: "01"})},
		{Key: graveler.Key("h/file2"), Value: catalog.MustEntryToValue(&catalog.Entry{Address: "h/file2", LastModified: timestamppb.New(now), Size: 2, ETag: "02"})},
	}
)

func TestUncommittedIterator(t *testing.T) {
	ctx := context.Background()
	gravelerMock := &catalog.FakeGraveler{
		ListIteratorFactory: catalog.NewFakeValueIteratorFactory(uncommittedBranchRecords),
	}
	const repoID = "uncommitted-iterator"
	repository := &graveler.RepositoryRecord{
		RepositoryID: repoID,
		Repository: &graveler.Repository{
			StorageNamespace: "mem://" + repoID,
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		},
	}
	tests := []struct {
		name     string
		branches []*graveler.BranchRecord
		records  map[graveler.StagingToken][]*graveler.ValueRecord
	}{
		{
			name:     "no branches",
			branches: []*graveler.BranchRecord{},
		},
		{
			name: "no values",
			branches: []*graveler.BranchRecord{
				{
					BranchID: "b1",
					Branch: &graveler.Branch{
						StagingToken: "bst1",
					},
				},
				{
					BranchID: "b2",
					Branch: &graveler.Branch{
						StagingToken: "bst2",
					},
				},
			},
		},
		{
			name: "first branch no staging",
			branches: []*graveler.BranchRecord{
				{
					BranchID: "b1",
					Branch: &graveler.Branch{
						StagingToken: "bst1",
					},
				},
				{
					BranchID: "b2",
					Branch: &graveler.Branch{
						StagingToken: "bst2",
					},
				},
				{
					BranchID: "b3",
					Branch:   &graveler.Branch{},
				},
				{
					BranchID: "b4",
					Branch: &graveler.Branch{
						StagingToken: "bst4",
					},
				},
			},
			records: map[graveler.StagingToken][]*graveler.ValueRecord{
				"bst2": uncommittedBranchRecords,
				"bst4": uncommittedBranchRecords,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gravelerMock.BranchIteratorFactory = testutil.NewFakeBranchIteratorFactory(tt.branches)
			gravelerMock.ListStagingIteratorFactory = catalog.NewFakeStagingIteratorFactory(tt.records)
			itr, err := catalog.NewUncommittedIterator(ctx, gravelerMock, repository)
			require.NoError(t, err)
			count := 0
			for itr.Next() {
				require.Equal(t, uncommittedBranchRecords[count%len(uncommittedBranchRecords)].Key.String(), itr.Value().Path.String())
				count += 1
			}
			expectedCount := len(tt.records) * len(uncommittedBranchRecords)
			require.Equal(t, expectedCount, count)
			require.NoError(t, itr.Err())
			require.False(t, itr.Next())
			require.NoError(t, itr.Err())
			itr.Close()
			require.NoError(t, itr.Err())
		})
	}
}

func TestUncommittedIterator_SeekGE(t *testing.T) {
	ctx := context.Background()
	gravelerMock := &catalog.FakeGraveler{
		ListIteratorFactory: catalog.NewFakeValueIteratorFactory(uncommittedBranchRecords),
	}
	const repoID = "uncommitted-iter-seek-ge"
	repository := &graveler.RepositoryRecord{
		RepositoryID: repoID,
		Repository: &graveler.Repository{
			StorageNamespace: "mem://" + repoID,
			CreationDate:     time.Now(),
			DefaultBranchID:  "main",
		},
	}

	tests := []struct {
		name          string
		branches      []*graveler.BranchRecord
		records       map[graveler.StagingToken][]*graveler.ValueRecord
		expectedCount int
	}{
		{
			name:          "no branches",
			branches:      []*graveler.BranchRecord{},
			expectedCount: 0,
		},
		{
			name: "no values",
			branches: []*graveler.BranchRecord{
				{
					BranchID: "b1",
					Branch: &graveler.Branch{
						StagingToken: "bst1",
					},
				},
				{
					BranchID: "b2",
					Branch: &graveler.Branch{
						StagingToken: "bst2",
					},
				},
			},
			expectedCount: 0,
		},
		{
			name: "basic seek",
			branches: []*graveler.BranchRecord{
				{
					BranchID: "b1",
					Branch: &graveler.Branch{
						StagingToken: "bst1",
					},
				},
				{
					BranchID: "b2",
					Branch: &graveler.Branch{
						StagingToken: "bst2",
					},
				},
				{
					BranchID: "b3",
					Branch: &graveler.Branch{
						StagingToken: "bst3",
					},
				},
				{
					BranchID: "b4",
					Branch: &graveler.Branch{
						StagingToken: "bst4",
					},
				},
			},
			records: map[graveler.StagingToken][]*graveler.ValueRecord{
				"bst1": uncommittedBranchRecords,
				"bst3": uncommittedBranchRecords,
				"bst4": uncommittedBranchRecords,
			},
			expectedCount: 2 * len(uncommittedBranchRecords),
		},
		{
			name: "seek next key after branch",
			branches: []*graveler.BranchRecord{
				{
					BranchID: "b1",
					Branch: &graveler.Branch{
						StagingToken: "bst1",
					},
				},
				{
					BranchID: "b2",
					Branch: &graveler.Branch{
						StagingToken: "bst2",
					},
				},
				{
					BranchID: "b3",
					Branch: &graveler.Branch{
						StagingToken: "bst3",
					},
				},
				{
					BranchID: "b4",
					Branch: &graveler.Branch{
						StagingToken: "bst4",
					},
				},
			},
			records: map[graveler.StagingToken][]*graveler.ValueRecord{
				"bst1": uncommittedBranchRecords,
				"bst4": uncommittedBranchRecords,
			},
			expectedCount: len(uncommittedBranchRecords),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gravelerMock.BranchIteratorFactory = testutil.NewFakeBranchIteratorFactory(tt.branches)
			gravelerMock.ListStagingIteratorFactory = catalog.NewFakeStagingIteratorFactory(tt.records)
			skipNum := rand.Intn(len(uncommittedBranchRecords))
			itr, err := catalog.NewUncommittedIterator(ctx, gravelerMock, repository)
			require.NoError(t, err)
			itr.SeekGE("b3", catalog.Path(uncommittedBranchRecords[skipNum].Key))
			require.NoError(t, err)
			count := 0
			offset := 0
			if len(tt.records["bst3"]) > 0 {
				offset = skipNum
			}
			for itr.Next() {
				require.Equal(t, uncommittedBranchRecords[(count+offset)%len(uncommittedBranchRecords)].Key.String(), itr.Value().Path.String())
				count += 1
			}
			expectedCount := tt.expectedCount
			expectedCount -= offset

			require.Equal(t, expectedCount, count)
			require.NoError(t, itr.Err())
			require.False(t, itr.Next())

			itr.SeekGE("b3", catalog.Path(uncommittedBranchRecords[skipNum].Key))
			require.NoError(t, itr.Err())
			itr.Close()
			require.NoError(t, itr.Err())
		})
	}
}
