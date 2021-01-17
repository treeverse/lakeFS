package committed_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/committed/mock"
	"github.com/treeverse/lakefs/testutil"
)

func getExpected(t *testing.T, record graveler.ValueRecord) committed.Record {
	t.Helper()
	expectedValue, err := committed.MarshalValue(record.Value)
	testutil.Must(t, err)
	return committed.Record{Key: committed.Key(record.Key), Value: expectedValue}
}

var params = committed.Params{
	MinRangeSizeBytes:          0,
	MaxRangeSizeBytes:          50_000,
	RangeSizeEntriesRaggedness: 100,
}

func TestWriter_WriteRecords(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := mock.NewMockRangeWriter(ctrl)
	mockWriter.EXPECT().AddMetadata(committed.MetadataTypeKey, committed.MetadataRangesType)
	// Never attempt to split files.
	mockWriter.EXPECT().GetApproximateSize().Return(uint64(0)).AnyTimes()
	writeResult := committed.WriteResult{
		RangeID: committed.ID("id"),
		First:   committed.Key("a"),
		Last:    committed.Key("z"),
	}
	mockWriter.EXPECT().Close().Return(&writeResult, nil)
	mockWriter.EXPECT().Abort().Return(nil).AnyTimes()
	rangeManager := mock.NewMockRangeManager(ctrl)

	rangeManager.EXPECT().GetWriter(gomock.Any(), gomock.Any()).Return(mockWriter, nil)

	mockMetaWriter := mock.NewMockRangeWriter(ctrl)
	rangeManagerMeta := mock.NewMockRangeManager(ctrl)
	rangeManagerMeta.EXPECT().GetWriter(gomock.Any(), gomock.Any()).Return(mockMetaWriter, nil)
	mockMetaWriter.EXPECT().AddMetadata(committed.MetadataTypeKey, committed.MetadataMetarangesType)
	mockMetaWriter.EXPECT().WriteRecord(gomock.Any()).AnyTimes()
	mockMetaWriter.EXPECT().GetApproximateSize().Return(uint64(1234)).AnyTimes()
	metaWriteResult := committed.WriteResult{
		RangeID: committed.ID("meta-range-id"),
		First:   committed.Key("a"),
		Last:    committed.Key("z"),
	}
	mockMetaWriter.EXPECT().Close().Return(&metaWriteResult, nil)
	mockMetaWriter.EXPECT().Abort().Return(nil).AnyTimes()
	namespace := committed.Namespace("ns")
	w := committed.NewGeneralMetaRangeWriter(ctx, rangeManager, rangeManagerMeta, &params, namespace)

	// Add first record
	firstRecord := graveler.ValueRecord{
		Key:   graveler.Key("c"),
		Value: &graveler.Value{},
	}
	expected := getExpected(t, firstRecord)
	mockWriter.EXPECT().WriteRecord(expected)
	err := w.WriteRecord(firstRecord)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}

	// Add second record
	secondRecord := graveler.ValueRecord{
		Key: graveler.Key("d"),
		Value: &graveler.Value{
			Identity: []byte("d"),
			Data:     nil,
		},
	}
	mockWriter.EXPECT().WriteRecord(getExpected(t, secondRecord))
	err = w.WriteRecord(secondRecord)
	if err != nil {
		t.Errorf("unexpected error %s", err)
	}
	// Fail on adding record with smaller key than previous key
	err = w.WriteRecord(graveler.ValueRecord{
		Key:   graveler.Key("cat"),
		Value: &graveler.Value{},
	})
	if !errors.Is(err, committed.ErrUnsortedKeys) {
		t.Errorf("expected ErrUnsorted got = %s", err)
	}

	_, err = w.Close()
	if err != nil {
		t.Errorf("failed to close: %s", err)
	}
}

func TestWriter_OverlappingRanges(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rangeManager := mock.NewMockRangeManager(ctrl)
	namespace := committed.Namespace("ns")
	rng := committed.Range{MinKey: committed.Key("a"), MaxKey: committed.Key("g")}
	rng2 := committed.Range{MinKey: committed.Key("c"), MaxKey: committed.Key("l")}
	w := committed.NewGeneralMetaRangeWriter(ctx, rangeManager, rangeManager, &params, namespace)
	err := w.WriteRange(rng)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
	err = w.WriteRange(rng2)
	if !errors.Is(err, committed.ErrUnsortedKeys) {
		t.Fatal("expected ErrUnsorted got = %w", err)
	}
}

func TestWriter_RecordRangeAndClose(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rangeManager := mock.NewMockRangeManager(ctrl)
	mockWriter := mock.NewMockRangeWriter(ctrl)

	rangeManagerMeta := mock.NewMockRangeManager(ctrl)
	mockMetaWriter := mock.NewMockRangeWriter(ctrl)

	namespace := committed.Namespace("ns")
	record := graveler.ValueRecord{Key: nil, Value: &graveler.Value{}}
	rng := committed.Range{ID: "rng2-id", MinKey: committed.Key("a"), MaxKey: committed.Key("g"), Count: 4}

	// get writer - once for record writer, once for range writer
	rangeManager.EXPECT().GetWriter(gomock.Any(), gomock.Any()).Return(mockWriter, nil)
	rangeManagerMeta.EXPECT().GetWriter(gomock.Any(), gomock.Any()).Return(mockMetaWriter, nil)

	// Never attempt to split files.
	mockWriter.EXPECT().GetApproximateSize().Return(uint64(0)).AnyTimes()
	mockMetaWriter.EXPECT().GetApproximateSize().Return(uint64(0)).AnyTimes()

	mockWriter.EXPECT().AddMetadata(committed.MetadataTypeKey, committed.MetadataRangesType)
	mockMetaWriter.EXPECT().AddMetadata(committed.MetadataTypeKey, committed.MetadataMetarangesType)

	// write two records on MetaRange and one for Range
	mockWriter.EXPECT().WriteRecord(gomock.Any())
	mockWriter.EXPECT().Close().Return(&committed.WriteResult{
		RangeID: "rng-id",
		First:   []byte("a"),
		Last:    []byte("a"),
		Count:   1,
	}, nil)
	mockMetaWriter.EXPECT().WriteRecord(getExpected(t, graveler.ValueRecord{
		Key: []byte("a"),
		Value: &graveler.Value{
			Identity: []byte("rng-id"),
			Data: mustMarshalRange(committed.Range{
				ID:     "rng-id",
				MinKey: []byte("a"),
				MaxKey: []byte("a"),
				Count:  1,
			}),
		},
	}))
	mockMetaWriter.EXPECT().WriteRecord(getExpected(t, graveler.ValueRecord{
		Key: []byte("g"),
		Value: &graveler.Value{
			Identity: []byte("rng2-id"),
			Data:     mustMarshalRange(rng),
		},
	}))
	mockMetaWriter.EXPECT().Close().Return(&committed.WriteResult{}, nil)
	mockWriter.EXPECT().Abort().AnyTimes()
	mockMetaWriter.EXPECT().Abort().AnyTimes()

	w := committed.NewGeneralMetaRangeWriter(ctx, rangeManager, rangeManagerMeta, &params, namespace)
	err := w.WriteRecord(record)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
	err = w.WriteRange(rng)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}

	_, err = w.Close()
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
}
