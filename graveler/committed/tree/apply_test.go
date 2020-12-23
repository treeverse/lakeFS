package tree_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/tree"
	"github.com/treeverse/lakefs/graveler/committed/tree/mock"
	"github.com/treeverse/lakefs/graveler/testutil"

	"github.com/golang/mock/gomock"
)

func makeV(k, id string) *graveler.ValueRecord {
	return &graveler.ValueRecord{Key: graveler.Key(k), Value: &graveler.Value{Identity: []byte(id)}}
}

func makeTombstoneV(k string) *graveler.ValueRecord {
	return &graveler.ValueRecord{Key: graveler.Key(k)}
}

type PV struct {
	P *tree.Part
	V *graveler.ValueRecord
}

type FakePartsAndValuesIterator struct {
	PV []PV
}

func NewFakePartsAndValuesIterator() *FakePartsAndValuesIterator {
	// Start with an empty record so the first `Next()` can skip it.
	return &FakePartsAndValuesIterator{PV: make([]PV, 1)}
}

func (i *FakePartsAndValuesIterator) AddPart(p *tree.Part) *FakePartsAndValuesIterator {
	i.PV = append(i.PV, PV{P: p})
	return i
}

func (i *FakePartsAndValuesIterator) AddValueRecords(vs ...*graveler.ValueRecord) *FakePartsAndValuesIterator {
	if len(i.PV) == 0 {
		panic(fmt.Sprintf("cannot add ValueRecords %+v with no part", vs))
	}
	part := i.PV[len(i.PV)-1].P
	for _, v := range vs {
		i.PV = append(i.PV, PV{P: part, V: v})
	}
	return i
}

func (i *FakePartsAndValuesIterator) Next() bool {
	if len(i.PV) <= 1 {
		return false
	}
	i.PV = i.PV[1:]
	return true
}

func (i *FakePartsAndValuesIterator) NextPart() bool {
	for {
		if len(i.PV) <= 1 {
			return false
		}
		i.PV = i.PV[1:]
		if i.PV[0].V == nil {
			return true
		}
	}
}

func (i *FakePartsAndValuesIterator) Value() (*graveler.ValueRecord, *tree.Part) {
	return i.PV[0].V, i.PV[0].P
}

func (i *FakePartsAndValuesIterator) Err() error { return nil }
func (i *FakePartsAndValuesIterator) Close()     {}

func TestApplyAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part2 := &tree.Part{Name: "two", MaxKey: graveler.Key("dz")}
	source := NewFakePartsAndValuesIterator()
	source.
		AddPart(&tree.Part{Name: "one", MaxKey: graveler.Key("cz")}).
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
	writer.EXPECT().AddParts(gomock.Eq([]tree.Part{*part2}))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "dest:f")))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}

func TestApplyReplace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part2 := &tree.Part{Name: "two", MaxKey: graveler.Key("dz")}
	source := NewFakePartsAndValuesIterator()
	source.
		AddPart(&tree.Part{Name: "one", MaxKey: graveler.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddPart(part2).
		AddValueRecords(makeV("d", "source:d")).
		AddPart(&tree.Part{Name: "three", MaxKey: graveler.Key("ez")}).
		AddValueRecords(makeV("e", "source:e"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeV("b", "dest:b"),
		*makeV("e", "dest:e"),
	})

	writer := mock.NewMockWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "dest:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().AddParts(gomock.Eq([]tree.Part{*part2}))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}

func TestApplyDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part2 := &tree.Part{Name: "two", MaxKey: graveler.Key("dz")}
	source := NewFakePartsAndValuesIterator()
	source.
		AddPart(&tree.Part{Name: "one", MaxKey: graveler.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddPart(part2).
		AddValueRecords(makeV("d", "source:d")).
		AddPart(&tree.Part{Name: "three", MaxKey: graveler.Key("ez")}).
		AddValueRecords(makeV("e", "source:e"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeTombstoneV("b"),
		*makeTombstoneV("e"),
	})

	writer := mock.NewMockWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().AddParts(gomock.Eq([]tree.Part{*part2}))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}

func TestApplyCopiesLeftoverDiffs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part2 := &tree.Part{Name: "two", MaxKey: graveler.Key("dz")}
	source := NewFakePartsAndValuesIterator()
	source.
		AddPart(&tree.Part{Name: "one", MaxKey: graveler.Key("cz")}).
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
	writer.EXPECT().AddParts(gomock.Eq([]tree.Part{*part2}))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "dest:f")))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}

func TestApplyCopiesLeftoverSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	part1 := &tree.Part{Name: "one", MaxKey: graveler.Key("cz")}
	part2 := &tree.Part{Name: "two", MaxKey: graveler.Key("dz")}
	part4 := &tree.Part{Name: "four", MaxKey: graveler.Key("hz")}
	source := NewFakePartsAndValuesIterator()
	source.
		AddPart(part1).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddPart(part2).
		AddValueRecords(makeV("d", "source:d")).
		AddPart(&tree.Part{Name: "three", MaxKey: graveler.Key("ez")}).
		AddValueRecords(makeV("e", "source:e"), makeV("f", "source:f")).
		AddPart(part4).
		AddValueRecords(makeV("g", "source:g"), makeV("h", "source:h"))

	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeTombstoneV("e"),
	})

	writer := mock.NewMockWriter(ctrl)
	writer.EXPECT().AddParts(gomock.Eq([]tree.Part{*part1}))
	writer.EXPECT().AddParts(gomock.Eq([]tree.Part{*part2}))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "source:f")))
	writer.EXPECT().AddParts(gomock.Eq([]tree.Part{*part4}))

	assert.NoError(t, tree.Apply(context.Background(), writer, source, diffs))
}
