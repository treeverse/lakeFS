package tree_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	partMock "github.com/treeverse/lakefs/graveler/committed/mock"
	"github.com/treeverse/lakefs/graveler/committed/tree"
	"github.com/treeverse/lakefs/testutil"
)

func getExpected(t *testing.T, record graveler.ValueRecord) committed.Record {
	t.Helper()
	expectedValue, err := tree.MarshalValue(record.Value)
	testutil.Must(t, err)
	return committed.Record{Key: committed.Key(record.Key), Value: expectedValue}
}

func TestWriter_WriteRecords(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partManager := partMock.NewMockPartManager(ctrl)
	mockWriter := partMock.NewMockWriter(ctrl)
	partManager.EXPECT().GetBatchManager()
	partManager.EXPECT().GetWriter(gomock.Any()).Return(mockWriter, nil).MinTimes(1)
	namespace := committed.Namespace("ns")
	w := tree.NewWriter(partManager, partManager, 100, namespace)

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
	if !errors.Is(err, tree.ErrUnsortedKeys) {
		t.Fatal("expected ErrUnsorted got = %w", err)
	}

}

func TestWriter_OverlappingParts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partManager := partMock.NewMockPartManager(ctrl)
	partManager.EXPECT().GetBatchManager()
	namespace := committed.Namespace("ns")
	part := tree.Part{MinKey: committed.Key("a"), MaxKey: committed.Key("g")}
	part2 := tree.Part{MinKey: committed.Key("c"), MaxKey: committed.Key("l")}
	w := tree.NewWriter(partManager, partManager, 100, namespace)
	err := w.AddPart(part)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
	err = w.AddPart(part2)
	if !errors.Is(err, tree.ErrUnsortedKeys) {
		t.Fatal("expected ErrUnsorted got = %w", err)
	}
}

func TestWriter_RecordPartAndClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partManager := partMock.NewMockPartManager(ctrl)
	mockBatchWriter := partMock.NewMockBatchWriterCloser(ctrl)
	partManager.EXPECT().GetBatchManager().Return(mockBatchWriter)
	mockBatchWriter.EXPECT().CloseWriterAsync(gomock.Any())
	mockBatchWriter.EXPECT().Wait()
	mockWriter := partMock.NewMockWriter(ctrl)
	namespace := committed.Namespace("ns")
	record := graveler.ValueRecord{Key: nil, Value: &graveler.Value{}}
	part := tree.Part{MinKey: committed.Key("a"), MaxKey: committed.Key("g")}
	// get writer - once for record writer, once for tree writer
	partManager.EXPECT().GetWriter(gomock.Any()).Return(mockWriter, nil).Times(2)

	mockWriter.EXPECT().WriteRecord(gomock.Any())
	mockWriter.EXPECT().Close().Return(&committed.WriteResult{}, nil)

	w := tree.NewWriter(partManager, partManager, 100, namespace)
	err := w.WriteRecord(record)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
	err = w.AddPart(part)
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

	partManager := partMock.NewMockPartManager(ctrl)
	mockBatchWriter := partMock.NewMockBatchWriterCloser(ctrl)
	mockWriter := partMock.NewMockWriter(ctrl)
	partManager.EXPECT().GetBatchManager().Return(mockBatchWriter)
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
			t.Error("parts written to tree in unsorted order")
		}
		lastKey = record.Key
	}).Times(3)
	partManager.EXPECT().GetWriter(gomock.Any()).Return(mockWriter, nil)
	mockWriter.EXPECT().Close().Return(&committed.WriteResult{PartID: ""}, nil)
	namespace := committed.Namespace("ns")
	w := tree.NewWriter(partManager, partManager, 100, namespace)
	_, err := w.Close()
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
}
