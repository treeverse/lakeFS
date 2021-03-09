package committed_test

import (
	"context"
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

	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	source := testutil.NewFakeIterator()
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

	summary, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{
			graveler.DiffTypeAdded: 3,
		},
	}, summary)
}

func TestApplyReplace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	source := testutil.NewFakeIterator()
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

	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	source := testutil.NewFakeIterator()
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

	summary, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{
			graveler.DiffTypeRemoved: 2,
		},
	}, summary)
}

func TestApplyCopiesLeftoverDiffs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	source := testutil.NewFakeIterator()
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

	range1 := &committed.Range{ID: "one", MaxKey: committed.Key("cz")}
	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	range4 := &committed.Range{ID: "four", MaxKey: committed.Key("hz")}
	source := testutil.NewFakeIterator()
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

	summary, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
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

	range1 := &committed.Range{ID: "one", MaxKey: committed.Key("cz")}
	range2 := &committed.Range{ID: "two", MaxKey: committed.Key("dz")}
	source := testutil.NewFakeIterator()
	source.
		AddRange(range1).
		AddValueRecords(makeV("a", "source:a"), makeV("b", "source:b"), makeV("c", "source:c")).
		AddRange(range2)

	diffs := testutil.NewValueIteratorFake([]graveler.ValueRecord{})

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRange(gomock.Any()).AnyTimes()

	_, err := committed.Apply(context.Background(), writer, source, diffs, &committed.ApplyOptions{})
	assert.Error(t, err, graveler.ErrNoChanges)
}
