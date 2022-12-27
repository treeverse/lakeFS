package committed_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/catalog/testutils"
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
		name    string
		records []*graveler.ValueRecord
	}{
		{
			name: "iterator_exhausted",
			records: []*graveler.ValueRecord{
				{Key: graveler.Key("1"), Value: &graveler.Value{}},
				{Key: graveler.Key("2"), Value: &graveler.Value{}},
			},
		},
		{
			name: "break_at_key",
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

			rangeWriter.EXPECT().Abort().Return(nil)
			rangeManager.EXPECT().GetWriter(context.Background(), committed.Namespace(ns), nil).Return(rangeWriter, nil)

			sut := committed.NewCommittedManager(metarangeManager, rangeManager, params)

			times := 0
			expectedTimes := min(len(tt.records), maxRecords)
			rangeWriter.EXPECT().WriteRecord(gomock.Any()).Return(nil).Times(expectedTimes)
			rangeWriter.EXPECT().ShouldBreakAtKey(gomock.Any(), gomock.Any()).
				DoAndReturn(func(interface{}, interface{}) bool { times++; return times == maxRecords }).Times(expectedTimes)
			rangeWriter.EXPECT().Close().Return(writeResult, nil)
			rangeWriter.EXPECT().SetMetadata(committed.MetadataTypeKey, committed.MetadataRangesType)

			it := testutils.NewFakeValueIterator(tt.records)
			rangeInfo, err := sut.WriteRange(context.Background(), ns, it)
			require.NoError(t, err)
			require.Equal(t, &graveler.RangeInfo{
				ID:                      graveler.RangeID(writeResult.RangeID),
				MinKey:                  graveler.Key(writeResult.First),
				MaxKey:                  graveler.Key(writeResult.Last),
				Count:                   writeResult.Count,
				EstimatedRangeSizeBytes: writeResult.EstimatedRangeSizeBytes,
			}, rangeInfo)
		})
	}
}

func TestManager_WriteMetaRange(t *testing.T) {
	const (
		ns = "some-ns"
	)

	expectedMetarangeID := graveler.MetaRangeID("some-id")

	tests := []struct {
		name    string
		records []*graveler.RangeInfo
	}{
		//{
		//	name: "in_order",
		//	records: []*graveler.RangeInfo{
		//		{ID: "id1", MinKey: graveler.Key("1"), MaxKey: graveler.Key("11")},
		//		{ID: "id2", MinKey: graveler.Key("2"), MaxKey: graveler.Key("22")},
		//	},
		//},
		{
			name: "wrong_order",
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
			metarangeWriter.EXPECT().Close().Return(&expectedMetarangeID, nil)
			metarangeWriter.EXPECT().Abort().Return(nil)
			sut := committed.NewCommittedManager(metarangeManager, rangeManager, params)

			actualMetarangeID, err := sut.WriteMetaRange(context.Background(), ns, tt.records)
			require.NoError(t, err)
			require.Equal(t, &graveler.MetaRangeInfo{ID: expectedMetarangeID}, actualMetarangeID)
		})
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
