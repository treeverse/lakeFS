package tree_test

import (
	"errors"
	"testing"

	"github.com/treeverse/lakefs/testutil"

	"github.com/treeverse/lakefs/graveler/committed"

	"github.com/treeverse/lakefs/graveler/committed/tree"

	"github.com/golang/mock/gomock"
	partMock "github.com/treeverse/lakefs/graveler/committed/mock"

	"github.com/treeverse/lakefs/graveler"
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
	partManager.EXPECT().GetWriter(gomock.Any()).Return(mockWriter, nil).MinTimes(1)
	namespace := committed.Namespace("ns")
	w := tree.NewWriter(partManager, 100, namespace, namespace)

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
	if !errors.Is(err, tree.ErrUnsorted) {
		t.Fatal("expected ErrUnsorted got = %w", err)
	}

}

func TestWriter_AddPart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partManager := partMock.NewMockPartManager(ctrl)
	//mockWriter := partMock.NewMockWriter(ctrl)
	mockTreeWriter := partMock.NewMockWriter(ctrl)
	namespace := committed.Namespace("ns")
	part := tree.Part{
		ID:                "",
		MinKey:            committed.Key("aaa"),
		MaxKey:            committed.Key("bbb"),
		EstimatedSize:     50,
		ReachedBrakePoint: false,
	}
	partRecord, err := tree.PartToTreeRecord(part)
	testutil.MustDo(t, "convert part to tree record", err)
	partManager.EXPECT().GetWriter(gomock.Any()).Return(mockTreeWriter, nil)
	mockTreeWriter.EXPECT().WriteRecord(*partRecord)
	w := tree.NewWriter(partManager, 100, namespace, namespace)
	err = w.AddPart(part)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
}

func TestWriter_OverlappingParts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partManager := partMock.NewMockPartManager(ctrl)
	mockTreeWriter := partMock.NewMockWriter(ctrl)
	namespace := committed.Namespace("ns")
	part := tree.Part{MinKey: committed.Key("a"), MaxKey: committed.Key("g")}
	part2 := tree.Part{MinKey: committed.Key("c"), MaxKey: committed.Key("l")}
	partManager.EXPECT().GetWriter(gomock.Any()).Return(mockTreeWriter, nil)
	mockTreeWriter.EXPECT().WriteRecord(gomock.Any())
	w := tree.NewWriter(partManager, 100, namespace, namespace)
	err := w.AddPart(part)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
	err = w.AddPart(part2)
	if !errors.Is(err, tree.ErrUnsorted) {
		t.Fatal("expected ErrUnsorted got = %w", err)
	}
}

func TestWriter_RecordPartAndSave(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	partManager := partMock.NewMockPartManager(ctrl)
	mockWriter := partMock.NewMockWriter(ctrl)
	namespace := committed.Namespace("ns")
	record := graveler.ValueRecord{Key: nil, Value: &graveler.Value{}}
	part := tree.Part{MinKey: committed.Key("a"), MaxKey: committed.Key("g")}
	// get writer - once for record writer, once for tree writer
	partManager.EXPECT().GetWriter(gomock.Any()).Return(mockWriter, nil).Times(2)
	// WriteRecord - first record, part created from record, part added
	mockWriter.EXPECT().WriteRecord(gomock.Any()).Times(3)
	mockWriter.EXPECT().Close().Return(&committed.WriteResult{}, nil).Times(2)

	w := tree.NewWriter(partManager, 100, namespace, namespace)
	err := w.WriteRecord(record)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
	err = w.AddPart(part)
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}

	_, err = w.SaveTree()
	if err != nil {
		t.Fatal("unexpected error %w", err)
	}
}
