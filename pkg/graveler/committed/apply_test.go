package committed_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/committed/mock"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
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

	source := testutil.NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 2}).
		AddValueRecords(makeV("a", "source:a"), makeV("c", "source:c")).
		AddRange(&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("d"), Count: 1}).
		AddValueRecords(makeV("d", "source:d"))
	diffs := testutil.NewFakeIterator()
	diffs.
		AddRange(&committed.Range{ID: "b", MinKey: committed.Key("b"), MaxKey: committed.Key("f"), Count: 3}).
		AddValueRecords(makeV("b", "dest:b"), makeV("e", "dest:e"), makeV("f", "dest:f"))
	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "dest:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().WriteRange(gomock.Eq(*&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("d"), Count: 1}))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "dest:f")))

	summary, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{
			graveler.DiffTypeAdded: 3,
		},
	}, summary)
}

func TestDiffRangesWithinSourceRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := testutil.NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "outer-range", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 2}).
		AddValueRecords(makeV("k1", "source:k1"), makeV("k6", "source:k6")).
		AddRange(&committed.Range{ID: "last", MinKey: committed.Key("k10"), MaxKey: committed.Key("k13"), Count: 2}).
		AddValueRecords(makeV("k10", "source:k10"), makeV("k13", "source:k13"))
	diffs := testutil.NewFakeIterator()
	diffs.
		AddRange(&committed.Range{ID: "inner-range-1", MinKey: committed.Key("k2"), MaxKey: committed.Key("k3"), Count: 2}).
		AddValueRecords(makeV("k2", "dest:k2"), makeV("k3", "dest:k3")).
		AddRange(&committed.Range{ID: "inner-range-2", MinKey: committed.Key("k4"), MaxKey: committed.Key("k5"), Count: 2}).
		AddValueRecords(makeV("k4", "dest:k4"), makeV("k5", "dest:k5"))
	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("k1", "source:k1")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("k2", "dest:k2")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("k3", "dest:k3")))
	writer.EXPECT().WriteRange(gomock.Eq(*&committed.Range{ID: "inner-range-2", MinKey: committed.Key("k4"), MaxKey: committed.Key("k5"), Count: 2}))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("k6", "source:k6")))
	writer.EXPECT().WriteRange(gomock.Eq(*&committed.Range{ID: "last", MinKey: committed.Key("k10"), MaxKey: committed.Key("k13"), Count: 2}))
	summary, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{
			graveler.DiffTypeAdded: 4,
		},
	}, summary)
}

func TestSourceRangesWithinDiffRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := testutil.NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "inner-range-1", MinKey: committed.Key("k2"), MaxKey: committed.Key("k3"), Count: 2}).
		AddValueRecords(makeV("k2", "source:k2"), makeV("k3", "source:k3")).
		AddRange(&committed.Range{ID: "inner-range-2", MinKey: committed.Key("k4"), MaxKey: committed.Key("k5"), Count: 2}).
		AddValueRecords(makeV("k4", "source:k4"), makeV("k5", "source:k5"))

	diffs := testutil.NewFakeIterator()
	diffs.
		AddRange(&committed.Range{ID: "outer-range", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 2}).
		AddValueRecords(makeV("k1", "diffs:k1"), makeV("k6", "diffs:k6")).
		AddRange(&committed.Range{ID: "last", MinKey: committed.Key("k10"), MaxKey: committed.Key("k13"), Count: 2}).
		AddValueRecords(makeV("k10", "diffs:k10"), makeV("k13", "diffs:k13"))
	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("k1", "diffs:k1")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("k2", "source:k2")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("k3", "source:k3")))
	writer.EXPECT().WriteRange(gomock.Eq(*&committed.Range{ID: "inner-range-2", MinKey: committed.Key("k4"), MaxKey: committed.Key("k5"), Count: 2}))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("k6", "diffs:k6")))
	writer.EXPECT().WriteRange(gomock.Eq(*&committed.Range{ID: "last", MinKey: committed.Key("k10"), MaxKey: committed.Key("k13"), Count: 2}))
	summary, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{
			graveler.DiffTypeAdded: 4,
		},
	}, summary)
}

func TestApplyReplace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := testutil.NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "source:one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}).
		AddValueRecords(makeV("d", "source:d")).
		AddRange(&committed.Range{ID: "three", MinKey: committed.Key("e"), MaxKey: committed.Key("ez"), Count: 1}).
		AddValueRecords(makeV("e", "source:e"))
	diffs := testutil.NewFakeIterator()
	diffs.
		AddRange(&committed.Range{ID: "diffs:one", MinKey: committed.Key("b"), MaxKey: committed.Key("e"), Count: 2}).
		AddValueRecords(makeV("b", "diffs:b"), makeV("e", "diffs:e"))

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "diffs:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().WriteRange(gomock.Eq(*&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "diffs:e")))

	summary, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{
			graveler.DiffTypeChanged: 2,
		},
	}, summary)
}

func TestApplyDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := testutil.NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}).
		AddValueRecords(makeV("d", "source:d")).
		AddRange(&committed.Range{ID: "three", MinKey: committed.Key("ez"), MaxKey: committed.Key("ez"), Count: 1}).
		AddValueRecords(makeV("e", "source:e"))
	diffs := testutil.NewFakeIterator()
	diffs.
		AddRange(&committed.Range{ID: "diffs:one", MinKey: committed.Key("b"), MaxKey: committed.Key("e"), Count: 2}).
		AddValueRecords(makeTombstoneV("b"), makeTombstoneV("e"))
	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().WriteRange(gomock.Eq(committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}))

	summary, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{
			graveler.DiffTypeRemoved: 2,
		},
	}, summary)
}

func TestAppCopiesLeftoverDiffs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	source := testutil.NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}).
		AddValueRecords(makeV("d", "source:d"))
	diffs := testutil.NewFakeIterator()
	diffs.
		AddRange(&committed.Range{ID: "diffs:one", MinKey: committed.Key("b"), MaxKey: committed.Key("f"), Count: 3}).
		AddValueRecords(makeV("b", "diffs:b"), makeV("e", "diffs:e"), makeV("f", "diffs:f"))

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("a", "source:a")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "diffs:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("c", "source:c")))
	writer.EXPECT().WriteRange(gomock.Eq(*&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "diffs:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "diffs:f")))
	summary, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{
			graveler.DiffTypeChanged: 1,
			graveler.DiffTypeAdded:   2,
		},
	}, summary)
}

func TestApplyCopiesLeftoverSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range1 := &committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}
	range2 := &committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}
	range4 := &committed.Range{ID: "four", MinKey: committed.Key("g"), MaxKey: committed.Key("hz"), Count: 2}
	source := testutil.NewFakeIterator()
	source.
		AddRange(range1).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "source:d")).
		AddRange(&committed.Range{ID: "three", MaxKey: committed.Key("ez"), Count: 1}).
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

	summary, err := committed.Apply(context.Background(), writer, source, committed.NewIteratorWrapper(diffs), &committed.ApplyOptions{})
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{
			graveler.DiffTypeRemoved: 1,
		},
	}, summary)
}

func TestApplyNoChangesFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range1 := &committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}
	range2 := &committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}
	source := testutil.NewFakeIterator()
	source.
		AddRange(range1).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "source:d"))

	diffs := testutil.NewFakeIterator()

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRange(gomock.Any()).AnyTimes()

	_, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.Error(t, err, graveler.ErrNoChanges)
}

func TestApplyCancelContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("source", func(t *testing.T) {
		source := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 2}).
			AddValueRecords(makeV("a", "source:a"), makeV("c", "source:c"))
		diffs := testutil.NewFakeIterator()
		writer := mock.NewMockMetaRangeWriter(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := committed.Apply(ctx, writer, source, diffs, &committed.ApplyOptions{})
		assert.True(t, errors.Is(err, context.Canceled), "context canceled error")
	})

	t.Run("diff", func(t *testing.T) {
		source := testutil.NewFakeIterator()
		diffs := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 1}).
			AddValueRecords(makeV("b", "dest:b"))
		writer := mock.NewMockMetaRangeWriter(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := committed.Apply(ctx, writer, source, diffs, &committed.ApplyOptions{})
		assert.True(t, errors.Is(err, context.Canceled), "context canceled error")
	})

	t.Run("source_and_diff", func(t *testing.T) {
		source := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
			AddValueRecords(makeV("a", "source:a"), makeV("c", "source:c"))
		diffs := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 1}).
			AddValueRecords(makeV("b", "dest:b"))
		writer := mock.NewMockMetaRangeWriter(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := committed.Apply(ctx, writer, source, diffs, &committed.ApplyOptions{})
		assert.True(t, errors.Is(err, context.Canceled), "context canceled error")
	})
}
