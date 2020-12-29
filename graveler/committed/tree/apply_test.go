package tree_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/committed/tree"
	"github.com/treeverse/lakefs/graveler/committed/tree/mock"
	"github.com/treeverse/lakefs/graveler/testutil"
)

func makeV(k, id string) *graveler.ValueRecord {
	return &graveler.ValueRecord{Key: graveler.Key(k), Value: &graveler.Value{Identity: []byte(id)}}
}

func makeTombstoneV(k string) *graveler.ValueRecord {
	return &graveler.ValueRecord{Key: graveler.Key(k)}
}

func TestApplyAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part2 := &tree.Part{ID: "two", MaxKey: committed.Key("dz")}
	source := testutil.NewFakePartsAndValuesIterator()
	source.
		AddPart(&tree.Part{ID: "one", MaxKey: committed.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("c", "source:c")).
		AddPart(part2).
		AddValueRecords(makeV("d", "source:d"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeV("b", "dest:b"),
		*makeV("e", "dest:e"),
		*makeV("f", "dest:f"),
	})

	writer := mock.NewMockWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "dest:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().AddPart(gomock.Eq(*part2))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "dest:f")))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}

func TestApplyReplace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part2 := &tree.Part{ID: "two", MaxKey: committed.Key("dz")}
	source := testutil.NewFakePartsAndValuesIterator()
	source.
		AddPart(&tree.Part{ID: "one", MaxKey: committed.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddPart(part2).
		AddValueRecords(makeV("d", "source:d")).
		AddPart(&tree.Part{ID: "three", MaxKey: committed.Key("ez")}).
		AddValueRecords(makeV("e", "source:e"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeV("b", "dest:b"),
		*makeV("e", "dest:e"),
	})

	writer := mock.NewMockWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "dest:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().AddPart(gomock.Eq(*part2))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}

func TestApplyDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part2 := &tree.Part{ID: "two", MaxKey: committed.Key("dz")}
	source := testutil.NewFakePartsAndValuesIterator()
	source.
		AddPart(&tree.Part{ID: "one", MaxKey: committed.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddPart(part2).
		AddValueRecords(makeV("d", "source:d")).
		AddPart(&tree.Part{ID: "three", MaxKey: committed.Key("ez")}).
		AddValueRecords(makeV("e", "source:e"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeTombstoneV("b"),
		*makeTombstoneV("e"),
	})

	writer := mock.NewMockWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().AddPart(gomock.Eq(*part2))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}

func TestApplyCopiesLeftoverDiffs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part2 := &tree.Part{ID: "two", MaxKey: committed.Key("dz")}
	source := testutil.NewFakePartsAndValuesIterator()
	source.
		AddPart(&tree.Part{ID: "one", MaxKey: committed.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddPart(part2).
		AddValueRecords(makeV("d", "source:d"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeV("b", "dest:b"),
		*makeV("e", "dest:e"),
		*makeV("f", "dest:f"),
	})

	writer := mock.NewMockWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "dest:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().AddPart(gomock.Eq(*part2))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "dest:f")))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}

func TestApplyCopiesLeftoverSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part1 := &tree.Part{ID: "one", MaxKey: committed.Key("cz")}
	part2 := &tree.Part{ID: "two", MaxKey: committed.Key("dz")}
	part4 := &tree.Part{ID: "four", MaxKey: committed.Key("hz")}
	source := testutil.NewFakePartsAndValuesIterator()
	source.
		AddPart(part1).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddPart(part2).
		AddValueRecords(makeV("d", "source:d")).
		AddPart(&tree.Part{ID: "three", MaxKey: committed.Key("ez")}).
		AddValueRecords(makeV("e", "source:e"), makeV("f", "source:f")).
		AddPart(part4).
		AddValueRecords(makeV("g", "source:g"), makeV("h", "source:h"))

	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeTombstoneV("e"),
	})

	writer := mock.NewMockWriter(ctrl)
	writer.EXPECT().AddPart(gomock.Eq(*part1))
	writer.EXPECT().AddPart(gomock.Eq(*part2))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "source:f")))
	writer.EXPECT().AddPart(gomock.Eq(*part4))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}
