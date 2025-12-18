package committed_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/committed/mock"
)

func TestManager_WriteRange(t *testing.T) {
	const (
		ns         = "some-ns"
		maxRecords = 4
	)

	writeResult := &committed.WriteResult{
		RangeID:                 "someid",
		First:                   committed.Key("1"),
		Last:                    committed.Key("10"),
		Count:                   2,
		EstimatedRangeSizeBytes: 23,
	}

	tests := []struct {
		name           string
		initStorageID  graveler.StorageID
		writeStorageID graveler.StorageID
		records        []*graveler.ValueRecord
		errorIs        error
	}{
		{
			name:           "iterator_exhausted",
			initStorageID:  config.SingleBlockstoreID,
			writeStorageID: config.SingleBlockstoreID,
			records: []*graveler.ValueRecord{
				{Key: graveler.Key("1"), Value: &graveler.Value{}},
				{Key: graveler.Key("2"), Value: &graveler.Value{}},
			},
		},
		{
			name:           "iterator_exhausted_with_non_single_sid",
			initStorageID:  "sid1",
			writeStorageID: "sid1",
			records: []*graveler.ValueRecord{
				{Key: graveler.Key("1"), Value: &graveler.Value{}},
				{Key: graveler.Key("2"), Value: &graveler.Value{}},
			},
		},
		{
			name:           "mismatched_sid",
			initStorageID:  config.SingleBlockstoreID,
			writeStorageID: "summat_else",
			records: []*graveler.ValueRecord{
				{Key: graveler.Key("1"), Value: &graveler.Value{}},
				{Key: graveler.Key("2"), Value: &graveler.Value{}},
			},
			errorIs: graveler.ErrInvalidStorageID,
		},
		{
			name:           "mismatched_sid_with_non_single_sid",
			initStorageID:  "sid1",
			writeStorageID: "summat_else",
			records: []*graveler.ValueRecord{
				{Key: graveler.Key("1"), Value: &graveler.Value{}},
				{Key: graveler.Key("2"), Value: &graveler.Value{}},
			},
			errorIs: graveler.ErrInvalidStorageID,
		},
		{
			name:           "break_at_key",
			initStorageID:  config.SingleBlockstoreID,
			writeStorageID: config.SingleBlockstoreID,
			records: []*graveler.ValueRecord{
				{Key: graveler.Key("1"), Value: &graveler.Value{}},
				{Key: graveler.Key("2"), Value: &graveler.Value{}},
				{Key: graveler.Key("3"), Value: &graveler.Value{}},
				{Key: graveler.Key("4"), Value: &graveler.Value{}},
				{Key: graveler.Key("5"), Value: &graveler.Value{}},
			},
		},
		{
			name:           "break_at_key_with_non_single_sid",
			initStorageID:  "sid1",
			writeStorageID: "sid1",
			records: []*graveler.ValueRecord{
				{Key: graveler.Key("1"), Value: &graveler.Value{}},
				{Key: graveler.Key("2"), Value: &graveler.Value{}},
				{Key: graveler.Key("3"), Value: &graveler.Value{}},
				{Key: graveler.Key("4"), Value: &graveler.Value{}},
				{Key: graveler.Key("5"), Value: &graveler.Value{}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			metarangeManager := mock.NewMockMetaRangeManager(ctrl)
			rangeManager := mock.NewMockRangeManager(ctrl)
			rangeWriter := mock.NewMockRangeWriter(ctrl)

			rangeManagers := make(map[graveler.StorageID]committed.RangeManager)
			rangeManagers[tt.initStorageID] = rangeManager
			metaRangeManagers := make(map[graveler.StorageID]committed.MetaRangeManager)
			metaRangeManagers[tt.initStorageID] = metarangeManager
			sut := committed.NewCommittedManager(metaRangeManagers, rangeManagers, nil, params)

			times := 0
			expectedTimes := min(len(tt.records), maxRecords)
			if tt.errorIs == nil {
				rangeWriter.EXPECT().Abort().Return(nil)
				rangeManager.EXPECT().GetWriter(context.Background(), committed.Namespace(ns), nil).Return(rangeWriter, nil)
				rangeWriter.EXPECT().WriteRecord(gomock.Any()).Return(nil).Times(expectedTimes)
				rangeWriter.EXPECT().ShouldBreakAtKey(gomock.Any(), gomock.Any()).
					DoAndReturn(func(interface{}, interface{}) bool { times++; return times == maxRecords }).Times(expectedTimes)
				rangeWriter.EXPECT().Close().Return(writeResult, nil)
				rangeWriter.EXPECT().SetMetadata(committed.MetadataTypeKey, committed.MetadataRangesType)
			}

			it := testutils.NewFakeValueIterator(tt.records)
			rangeInfo, err := sut.WriteRange(context.Background(), tt.writeStorageID, ns, it)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
				require.Equal(t, &graveler.RangeInfo{
					ID:                      graveler.RangeID(writeResult.RangeID),
					MinKey:                  graveler.Key(writeResult.First),
					MaxKey:                  graveler.Key(writeResult.Last),
					Count:                   writeResult.Count,
					EstimatedRangeSizeBytes: writeResult.EstimatedRangeSizeBytes,
				}, rangeInfo)
			}
		})
	}
}

func TestManager_WriteMetaRange(t *testing.T) {
	const ns = "some-ns"

	expectedMetarangeID := graveler.MetaRangeID("some-id")

	tests := []struct {
		name           string
		initStorageID  graveler.StorageID
		writeStorageID graveler.StorageID
		records        []*graveler.RangeInfo
		errorIs        error
	}{
		{
			name:           "simple_write",
			initStorageID:  "",
			writeStorageID: "",
			records: []*graveler.RangeInfo{
				{ID: "id1", MinKey: graveler.Key("1"), MaxKey: graveler.Key("11")},
				{ID: "id2", MinKey: graveler.Key("2"), MaxKey: graveler.Key("22")},
				{ID: "id3", MinKey: graveler.Key("3"), MaxKey: graveler.Key("33")},
			},
		},
		{
			name:           "simple_write_alternate_storageID",
			initStorageID:  "summat_else",
			writeStorageID: "summat_else",
			records: []*graveler.RangeInfo{
				{ID: "id1", MinKey: graveler.Key("1"), MaxKey: graveler.Key("11")},
				{ID: "id2", MinKey: graveler.Key("2"), MaxKey: graveler.Key("22")},
				{ID: "id3", MinKey: graveler.Key("3"), MaxKey: graveler.Key("33")},
			},
		},
		{
			name:           "mismatched_sid",
			initStorageID:  "",
			writeStorageID: "summat_else",
			records: []*graveler.RangeInfo{
				{ID: "id1", MinKey: graveler.Key("1"), MaxKey: graveler.Key("11")},
				{ID: "id2", MinKey: graveler.Key("2"), MaxKey: graveler.Key("22")},
			},
			errorIs: graveler.ErrInvalidStorageID,
		},
		{
			name:           "wrong_order",
			initStorageID:  "",
			writeStorageID: "",
			records: []*graveler.RangeInfo{
				{ID: "id1", MinKey: graveler.Key("1"), MaxKey: graveler.Key("11")},
				{ID: "id3", MinKey: graveler.Key("3"), MaxKey: graveler.Key("33")},
				{ID: "id2", MinKey: graveler.Key("2"), MaxKey: graveler.Key("22")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			metarangeManager := mock.NewMockMetaRangeManager(ctrl)
			rangeManager := mock.NewMockRangeManager(ctrl)
			metarangeWriter := mock.NewMockMetaRangeWriter(ctrl)

			if tt.errorIs == nil {
				minKey := ""
				metarangeManager.EXPECT().NewWriter(context.Background(), graveler.StorageNamespace(ns), nil).Return(metarangeWriter)
				metarangeWriter.EXPECT().WriteRange(gomock.Any()).Return(nil).
					DoAndReturn(func(info committed.Range) error {
						if string(info.MinKey) < minKey {
							t.Fatalf("record should be sorted ascending - previous minKey '%s', current '%s'", minKey, info.MinKey)
						}
						minKey = string(info.MinKey)
						return nil
					}).Times(len(tt.records))
				metarangeWriter.EXPECT().Close(gomock.Any()).Return(&expectedMetarangeID, nil)
				metarangeWriter.EXPECT().Abort().Return(nil)
			}

			rangeManagers := make(map[graveler.StorageID]committed.RangeManager)
			rangeManagers[tt.initStorageID] = rangeManager
			metaRangeManagers := make(map[graveler.StorageID]committed.MetaRangeManager)
			metaRangeManagers[tt.initStorageID] = metarangeManager
			sut := committed.NewCommittedManager(metaRangeManagers, rangeManagers, nil, params)

			actualMetarangeID, err := sut.WriteMetaRange(context.Background(), tt.writeStorageID, ns, tt.records)
			if tt.errorIs != nil {
				require.ErrorIs(t, err, tt.errorIs)
			} else {
				require.NoError(t, err)
				require.Equal(t, &graveler.MetaRangeInfo{ID: expectedMetarangeID}, actualMetarangeID)
			}
		})
	}
}

func TestManager_CompareSourceEqualsBase(t *testing.T) {
	// Test that when source == base (comparing with an ancestor), the Compare function
	// returns raw diff results instead of filtering them. This is the fix for issue #9802
	// where comparing a branch with one of its ancestor commits would return empty diff.
	const (
		ns         = "test-ns"
		storageID  = graveler.StorageID("")
		destID     = graveler.MetaRangeID("dest-metarange")
		sourceID   = graveler.MetaRangeID("source-metarange")
		baseID     = graveler.MetaRangeID("source-metarange") // same as source - this is the case we're testing
	)

	ctrl := gomock.NewController(t)
	metarangeManager := mock.NewMockMetaRangeManager(ctrl)
	rangeManager := mock.NewMockRangeManager(ctrl)

	rangeManagers := make(map[graveler.StorageID]committed.RangeManager)
	rangeManagers[storageID] = rangeManager
	metaRangeManagers := make(map[graveler.StorageID]committed.MetaRangeManager)
	metaRangeManagers[storageID] = metarangeManager

	// Setup mock iterators - one for dest, one for source
	destIt := mock.NewMockIterator(ctrl)
	sourceIt := mock.NewMockIterator(ctrl)

	// Set up expectations for creating the iterators
	metarangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), graveler.StorageNamespace(ns), destID).Return(destIt, nil)
	metarangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), graveler.StorageNamespace(ns), sourceID).Return(sourceIt, nil)

	// For this test, we just need to verify the function returns without error
	// and doesn't try to create a base iterator (since source == base should short-circuit)
	// The key is that we're NOT expecting a third NewMetaRangeIterator call for base

	// Set up the iterators to end immediately for simplicity
	destIt.EXPECT().Next().Return(false).AnyTimes()
	destIt.EXPECT().Err().Return(nil).AnyTimes()
	destIt.EXPECT().Close().AnyTimes()
	sourceIt.EXPECT().Next().Return(false).AnyTimes()
	sourceIt.EXPECT().Err().Return(nil).AnyTimes()
	sourceIt.EXPECT().Close().AnyTimes()

	sut := committed.NewCommittedManager(metaRangeManagers, rangeManagers, nil, params)

	// Call Compare with source == base
	diffIt, err := sut.Compare(context.Background(), storageID, ns, destID, sourceID, baseID)
	require.NoError(t, err)
	require.NotNil(t, diffIt)

	// Iterate through the results (should be empty for this test case)
	for diffIt.Next() {
		// consume results
	}
	require.NoError(t, diffIt.Err())
	diffIt.Close()
}
