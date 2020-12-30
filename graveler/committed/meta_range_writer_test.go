package committed_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/graveler/committed/mock"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/testutil"
)

func getExpected(t *testing.T, record graveler.ValueRecord) committed.Record {
	t.Helper()
	expectedValue, err := committed.MarshalValue(record.Value)
	testutil.Must(t, err)
	return committed.Record{Key: committed.Key(record.Key), Value: expectedValue}
}

func TestWriter_WriteRecords(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rangeManager := mock.NewMockRangeManager(ctrl)
	mockWriter := mock.NewMockRangeWriter(ctrl)
	rangeManager.EXPECT().GetBatchWriter()
	rangeManager.EXPECT().GetWriter(gomock.Any()).Return(mockWriter, nil).MinTimes(1)
	namespace := committed.Namespace("ns")
	w := committed.NewGeneralMetaRangeWriter(rangeManager, rangeManager, 100, namespace)

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
		t.Fatal("unexpected error %w", err)
	}
	// Fail on adding record with smaller key than previous key
	err = w.WriteRecord(graveler.ValueRecord{
		Key:   graveler.Key("cat"),
		Value: &graveler.Value{},
	})
	if !errors.Is(err, committed.ErrUnsortedKeys) {
		t.Fatal("expected ErrUnsorted got = %w", err)
	}

}

func TestWriter_OverlappingRanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rangeManager := mock.NewMockRangeManager(ctrl)
	rangeManager.EXPECT().GetBatchWriter()
	namespace := committed.Namespace("ns")
	rng := committed.Range{MinKey: committed.Key("a"), MaxKey: committed.Key("g")}
	rng2 := committed.Range{MinKey: committed.Key("c"), MaxKey: committed.Key("l")}
	w := committed.NewGeneralMetaRangeWriter(rangeManager, rangeManager, 100, namespace)
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rangeManager := mock.NewMockRangeManager(ctrl)
	mockBatchWriter := mock.NewMockBatchWriterCloser(ctrl)
	rangeManager.EXPECT().GetBatchWriter().Return(mockBatchWriter)
	mockBatchWriter.EXPECT().CloseWriterAsync(gomock.Any())
	mockBatchWriter.EXPECT().Wait()
	mockWriter := mock.NewMockRangeWriter(ctrl)
	namespace := committed.Namespace("ns")
	record := graveler.ValueRecord{Key: nil, Value: &graveler.Value{}}
	rng := committed.Range{MinKey: committed.Key("a"), MaxKey: committed.Key("g")}
	// get writer - once for record writer, once for range writer
	rangeManager.EXPECT().GetWriter(gomock.Any()).Return(mockWriter, nil).Times(2)
	// write two records on MetaRange and one for Range
	mockWriter.EXPECT().WriteRecord(gomock.Any()).Times(2)
	mockWriter.EXPECT().Abort()
	mockWriter.EXPECT().Close().Return(&committed.WriteResult{}, nil)

	w := committed.NewGeneralMetaRangeWriter(rangeManager, rangeManager, 100, namespace)
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

func TestWriter_SortOnClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rangeManager := mock.NewMockRangeManager(ctrl)
	mockBatchWriter := mock.NewMockBatchWriterCloser(ctrl)
	mockWriter := mock.NewMockRangeWriter(ctrl)
	rangeManager.EXPECT().GetBatchWriter().Return(mockBatchWriter)
	mockBatchWriter.EXPECT().Wait().Return([]committed.WriteResult{
		{
			First: committed.Key("c"),
		},
		{
			First: committed.Key("ab"),
		},
		{
			First: committed.Key("b"),
		},
	}, nil)
	var lastKey committed.Key
	mockWriter.EXPECT().WriteRecord(gomock.Any()).Do(func(record committed.Record) {
		if bytes.Compare(record.Key, lastKey) <= 0 {
			t.Error("ranges written in unsorted order")
		}
		lastKey = record.Key
	}).Times(3)
	rangeManager.EXPECT().GetWriter(gomock.Any()).Return(mockWriter, nil)
	mockWriter.EXPECT().Close().Return(&committed.WriteResult{RangeID: ""}, nil)
	mockWriter.EXPECT().Abort()
	namespace := committed.Namespace("ns")
	w := committed.NewGeneralMetaRangeWriter(rangeManager, rangeManager, 100, namespace)
	_, err := w.Close()
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
}
