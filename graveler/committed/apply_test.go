package committed_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/committed/mock"
	"github.com/treeverse/lakefs/graveler/testutil"
)

func makeV(k, id string) *graveler.ValueRecord {
	return &graveler.ValueRecord{Key: graveler.Key(k), Value: &graveler.Value{Identity: []byte(id)}}
}

func makeTombstoneV(k string) *graveler.ValueRecord {
	return &graveler.ValueRecord{Key: graveler.Key(k)}
}

type PV struct {
	P *committed.Range
	V *graveler.ValueRecord
}

type FakeIterator struct {
	PV []PV
}

func NewFakeIterator() *FakeIterator {
	// Start with an empty record so the first `Next()` can skip it.
	return &FakeIterator{PV: make([]PV, 1)}
}

func (i *FakeIterator) AddRange(p *committed.Range) *FakeIterator {
	i.PV = append(i.PV, PV{P: p})
	return i
}

func (i *FakeIterator) AddValueRecords(vs ...*graveler.ValueRecord) *FakeIterator {
	if len(i.PV) == 0 {
		panic(fmt.Sprintf("cannot add ValueRecords %+v with no range", vs))
	}
	rng := i.PV[len(i.PV)-1].P
	for _, v := range vs {
		i.PV = append(i.PV, PV{P: rng, V: v})
	}
	return i
}

func (i *FakeIterator) Next() bool {
	if len(i.PV) <= 1 {
		return false
	}
	i.PV = i.PV[1:]
	return true
}

func (i *FakeIterator) NextRange() bool {
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

func (i *FakeIterator) Value() (*graveler.ValueRecord, *committed.Range) {
	return i.PV[0].V, i.PV[0].P
}

func (i *FakeIterator) Err() error { return nil }
func (i *FakeIterator) Close()     {}

func TestApplyAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	source := NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "one", MaxKey: committed.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("c", "source:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "source:d"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeV("b", "dest:b"),
		*makeV("e", "dest:e"),
		*makeV("f", "dest:f"),
	})

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "dest:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().WriteRange(gomock.Eq(*range2))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "dest:f")))

	assert.NoError(t, committed.Apply(context.Background(), writer, source, diffs))
}

func TestApplyReplace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	source := NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "one", MaxKey: committed.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "source:d")).
		AddRange(&committed.Range{ID: "three", MaxKey: committed.Key("ez")}).
		AddValueRecords(makeV("e", "source:e"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeV("b", "dest:b"),
		*makeV("e", "dest:e"),
	})

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "dest:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().WriteRange(gomock.Eq(*range2))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))

	assert.NoError(t, committed.Apply(context.Background(), writer, source, diffs))
}

func TestApplyDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	source := NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "one", MaxKey: committed.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "source:d")).
		AddRange(&committed.Range{ID: "three", MaxKey: committed.Key("ez")}).
		AddValueRecords(makeV("e", "source:e"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeTombstoneV("b"),
		*makeTombstoneV("e"),
	})

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().WriteRange(gomock.Eq(*range2))

	assert.NoError(t, committed.Apply(context.Background(), writer, source, diffs))
}

func TestApplyCopiesLeftoverDiffs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	source := NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "one", MaxKey: committed.Key("cz")}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "source:d"))
	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeV("b", "dest:b"),
		*makeV("e", "dest:e"),
		*makeV("f", "dest:f"),
	})

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "dest:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().WriteRange(gomock.Eq(*range2))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "dest:f")))

	assert.NoError(t, committed.Apply(context.Background(), writer, source, diffs))
}

func TestApplyCopiesLeftoverSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range1 := &committed.Range{ID: "one", MaxKey: committed.Key("cz")}
	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	range4 := &committed.Range{ID: "four", MaxKey: committed.Key("hz")}
	source := NewFakeIterator()
	source.
		AddRange(range1).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "source:d")).
		AddRange(&committed.Range{ID: "three", MaxKey: committed.Key("ez")}).
		AddValueRecords(makeV("e", "source:e"), makeV("f", "source:f")).
		AddRange(range4).
		AddValueRecords(makeV("g", "source:g"), makeV("h", "source:h"))

	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{
		*makeTombstoneV("e"),
	})

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRange(gomock.Eq(*range1))
	writer.EXPECT().WriteRange(gomock.Eq(*range2))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "source:f")))
	writer.EXPECT().WriteRange(gomock.Eq(*range4))

	assert.NoError(t, committed.Apply(context.Background(), writer, source, diffs))
}
