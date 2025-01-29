package committed_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/committed/mock"
)

func Test_import(t *testing.T) {
	tests := []struct {
		name           string
		sourceRange    *testMetaRange
		destRange      *testMetaRange
		prefixes       []graveler.Prefix
		expectedResult []testRunResult
	}{
		{
			name: "source range smaller than dest range",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"a/1", "a1"}, {"a/2", "a2"},
					},
				},
				{
					rng: committed.Range{ID: "a/3-a/6", MinKey: committed.Key("a/3"), MaxKey: committed.Key("a/6"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"a/3", "a3"}, {"a/4", "a4"}, {"a/5", "a5"}, {"a/6", "a6"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/7-a/9", MinKey: committed.Key("a/7"), MaxKey: committed.Key("a/9"), Count: 3, EstimatedSize: 1536},
					records: []testValueRecord{
						{"a/7", "a7"}, {"a/8", "a8"}, {"a/9", "a9"},
					},
				},
			}),
			prefixes: []graveler.Prefix{
				"a",
			},
			expectedResult: []testRunResult{
				{
					expectedActions: []writeAction{
						{
							action: actionTypeWriteRange,
							rng:    committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
						},
						{
							action: actionTypeWriteRange,
							rng:    committed.Range{ID: "a/3-a/6", MinKey: committed.Key("a/3"), MaxKey: committed.Key("a/6"), Count: 4, EstimatedSize: 2048},
						},
					},
				},
			},
		},
		{
			name: "dest range smaller than compared prefix",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "b/1-b/2", MinKey: committed.Key("b/1"), MaxKey: committed.Key("b/2"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"b/1", "b1"}, {"b/2", "b2"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"a/1", "a1"}, {"a/2", "a2"},
					},
				},
				{
					rng: committed.Range{ID: "a/3-a/6", MinKey: committed.Key("a/3"), MaxKey: committed.Key("a/6"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"a/3", "a3"}, {"a/4", "a4"}, {"a/5", "a5"}, {"a/6", "a6"},
					},
				},
			}),
			prefixes: []graveler.Prefix{
				"b",
			},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "a/3-a/6", MinKey: committed.Key("a/3"), MaxKey: committed.Key("a/6"), Count: 4, EstimatedSize: 2048},
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "b/1-b/2", MinKey: committed.Key("b/1"), MaxKey: committed.Key("b/2"), Count: 2, EstimatedSize: 1024},
					},
				},
			}},
		},
		{
			name: "same range",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"a/1", "a1"}, {"a/2", "a2"},
					},
				},
				{
					rng: committed.Range{ID: "a/3-a/6", MinKey: committed.Key("a/3"), MaxKey: committed.Key("a/6"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"a/3", "a3"}, {"a/4", "a4"}, {"a/5", "a5"}, {"a/6", "a6"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"a/1", "a1"}, {"a/2", "a2"},
					},
				},
				{
					rng: committed.Range{ID: "a/3-a/6", MinKey: committed.Key("a/3"), MaxKey: committed.Key("a/6"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"a/3", "a3"}, {"a/4", "a4"}, {"a/5", "a5"}, {"a/6", "a6"},
					},
				},
				{
					rng: committed.Range{ID: "c/4-c/7", MinKey: committed.Key("c/4"), MaxKey: committed.Key("c/7"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"c/4", "c4"}, {"c/7", "c7"},
					},
				},
			}),
			prefixes: []graveler.Prefix{
				"a",
			},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "a/3-a/6", MinKey: committed.Key("a/3"), MaxKey: committed.Key("a/6"), Count: 4, EstimatedSize: 2048},
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "c/4-c/7", MinKey: committed.Key("c/4"), MaxKey: committed.Key("c/7"), Count: 2, EstimatedSize: 1024},
					},
				},
			}},
		},
		{
			name: "dest range is bounded by compared prefix",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/4-a/8", MinKey: committed.Key("a/4"), MaxKey: committed.Key("a/8"), Count: 5, EstimatedSize: 2560},
					records: []testValueRecord{
						{"a/4", "a4"}, {"a/5", "a5"}, {"a/6", "a6"}, {"a/7", "a7"}, {"a/8", "a8"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/1-a/6", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/6"), Count: 6, EstimatedSize: 3072},
					records: []testValueRecord{
						{"a/1", "a1"}, {"a/2", "a2"}, {"a/3", "a3"}, {"a/4", "a4"}, {"a/5", "a5"}, {"a/6", "a6"},
					},
				},
				{
					rng: committed.Range{ID: "b/1-b/6", MinKey: committed.Key("b/1"), MaxKey: committed.Key("b/6"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"b/1", "b1"}, {"b/2", "b2"}, {"b/3", "b3"}, {"b/4", "b4"}, {"b/5", "b5"}, {"b/6", "b6"},
					},
				},
			}),
			prefixes: []graveler.Prefix{
				"a",
			},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "a/4-a/8", MinKey: committed.Key("a/4"), MaxKey: committed.Key("a/8"), Count: 5, EstimatedSize: 2560},
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "b/1-b/6", MinKey: committed.Key("b/1"), MaxKey: committed.Key("b/6"), Count: 2, EstimatedSize: 1024},
					},
				},
			}},
		},
		{
			name: "multiple ranges in dest are bounded by compared prefix",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/4-a/8", MinKey: committed.Key("a/4"), MaxKey: committed.Key("a/8"), Count: 5, EstimatedSize: 2560},
					records: []testValueRecord{
						{"a/4", "a4"}, {"a/5", "a5"}, {"a/6", "a6"}, {"a/7", "a7"}, {"a/8", "a8"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/1-a/6", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/6"), Count: 6, EstimatedSize: 3072},
					records: []testValueRecord{
						{"a/1", "a1"}, {"a/2", "a2"}, {"a/3", "a3"}, {"a/4", "a4"}, {"a/5", "a5"}, {"a/6", "a6"},
					},
				},
				{
					rng: committed.Range{ID: "a/11-a/16", MinKey: committed.Key("a/11"), MaxKey: committed.Key("a/16"), Count: 6, EstimatedSize: 3072},
					records: []testValueRecord{
						{"a/11", "a11"}, {"a/12", "a12"}, {"a/13", "a13"}, {"a/14", "a14"}, {"a/15", "a15"}, {"a/16", "a16"},
					},
				},
				{
					rng: committed.Range{ID: "a/21-a/26", MinKey: committed.Key("a/21"), MaxKey: committed.Key("a/26"), Count: 6, EstimatedSize: 3072},
					records: []testValueRecord{
						{"a/21", "a21"}, {"a/22", "a22"}, {"a/23", "a23"}, {"a/24", "a24"}, {"a/25", "a25"}, {"a/26", "a26"},
					},
				},
				{
					rng: committed.Range{ID: "b/1-b/6", MinKey: committed.Key("b/1"), MaxKey: committed.Key("b/6"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"b/1", "b1"}, {"b/2", "b2"}, {"b/3", "b3"}, {"b/4", "b4"}, {"b/5", "b5"}, {"b/6", "b6"},
					},
				},
			}),
			prefixes: []graveler.Prefix{
				"a",
			},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "a/4-a/8", MinKey: committed.Key("a/4"), MaxKey: committed.Key("a/8"), Count: 5, EstimatedSize: 2560},
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "b/1-b/6", MinKey: committed.Key("b/1"), MaxKey: committed.Key("b/6"), Count: 2, EstimatedSize: 1024},
					},
				},
			}},
		},
		{
			name: "intersected ranges - multiple prefixes",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/1-a/4", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/4"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"a/1", "a1"}, {"a/2", "a2"}, {"a/3", "a3"}, {"a/4", "a4"},
					},
				},
				{
					rng: committed.Range{ID: "a/8-c/3", MinKey: committed.Key("a/8"), MaxKey: committed.Key("c/3"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"a/8", "a8"}, {"c/3", "c3"},
					},
				},
				{
					rng: committed.Range{ID: "c/4-c/7", MinKey: committed.Key("c/4"), MaxKey: committed.Key("c/7"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"c/4", "c4"}, {"c/7", "c7"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "a/2-a/5", MinKey: committed.Key("a/2"), MaxKey: committed.Key("a/5"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"a/2", "a2"}, {"a/3", "a3"}, {"a/4", "a4"}, {"a/5", "a5"},
					},
				},
				{
					rng: committed.Range{ID: "a/12-a/15", MinKey: committed.Key("a/12"), MaxKey: committed.Key("a/15"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"a/12", "a12"}, {"a/13", "a13"}, {"a/14", "a14"}, {"a/15", "a15"},
					},
				},
				{
					rng: committed.Range{ID: "a/8-b/4", MinKey: committed.Key("a/8"), MaxKey: committed.Key("b/4"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"a/8", "a8"}, {"b/1", "b1"}, {"b/2", "b2"}, {"b/4", "b4"},
					},
				},
				{
					rng: committed.Range{ID: "b/6-b/7", MinKey: committed.Key("b/6"), MaxKey: committed.Key("b/7"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{
						{"b/6", "b6"}, {"b/7", "b7"},
					},
				},
				{
					rng: committed.Range{ID: "b/8-c/2", MinKey: committed.Key("b/8"), MaxKey: committed.Key("c/2"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"b/8", "b8"}, {"b/9", "b9"}, {"c/1", "c1"}, {"c/2", "c2"},
					},
				},
				{
					rng: committed.Range{ID: "c/5-d/2", MinKey: committed.Key("c/5"), MaxKey: committed.Key("d/2"), Count: 4, EstimatedSize: 2048},
					records: []testValueRecord{
						{"c/5", "c5"}, {"c/6", "c6"}, {"d/1", "d1"}, {"d/2", "d2"},
					},
				},
			}),
			prefixes: []graveler.Prefix{
				"a",
				"c",
			},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "a/1-a/4", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/4"), Count: 4, EstimatedSize: 2048},
					},
					{action: actionTypeWriteRecord, key: "a/8", identity: "a8"},
					{action: actionTypeWriteRecord, key: "b/1", identity: "b1"},
					{action: actionTypeWriteRecord, key: "b/2", identity: "b2"},
					{action: actionTypeWriteRecord, key: "b/4", identity: "b4"},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "b/6-b/7", MinKey: committed.Key("b/6"), MaxKey: committed.Key("b/7"), Count: 2, EstimatedSize: 1024},
					},
					{action: actionTypeWriteRecord, key: "b/8", identity: "b8"},
					{action: actionTypeWriteRecord, key: "b/9", identity: "b9"},
					{action: actionTypeWriteRecord, key: "c/3", identity: "c3"},
					{action: actionTypeWriteRecord, key: "c/4", identity: "c4"},
					{action: actionTypeWriteRecord, key: "c/7", identity: "c7"},
					{action: actionTypeWriteRecord, key: "d/1", identity: "d1"},
					{action: actionTypeWriteRecord, key: "d/2", identity: "d2"},
				},
			}},
		},
	}

	for _, tst := range tests {
		for _, expectedResult := range tst.expectedResult {
			t.Run(tst.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				ctx := context.Background()
				writer := mock.NewMockMetaRangeWriter(ctrl)
				for _, action := range expectedResult.expectedActions {
					switch action.action {
					case actionTypeWriteRecord:
						writer.EXPECT().WriteRecord(newRecordMatcher(action.key, action.identity))
					case actionTypeWriteRange:
						writer.EXPECT().WriteRange(gomock.Eq(action.rng))
					}
				}
				metaRangeManager := mock.NewMockMetaRangeManager(ctrl)
				metaRangeManager.EXPECT().NewWriter(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(writer)
				sourceMetaRangeID := tst.sourceRange.GetMetaRangeID()
				destMetaRangeID := tst.destRange.GetMetaRangeID()
				metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), gomock.Any(), graveler.MetaRangeID("")).AnyTimes().Return(committed.NewEmptyIterator(), nil) // empty base
				metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), gomock.Any(), sourceMetaRangeID).AnyTimes().Return(createIter(tst.sourceRange), nil)
				metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), gomock.Any(), destMetaRangeID).AnyTimes().Return(createIter(tst.destRange), nil)

				rangeManager := mock.NewMockRangeManager(ctrl)

				writer.EXPECT().Abort().AnyTimes()
				metaRangeId := graveler.MetaRangeID("import")
				writer.EXPECT().Close(gomock.Any()).Return(&metaRangeId, nil).AnyTimes()
				committedManager := committed.NewCommittedManager(metaRangeManager, rangeManager, params)
				_, err := committedManager.Import(ctx, "", "ns", destMetaRangeID, sourceMetaRangeID, tst.prefixes)
				if !errors.Is(err, expectedResult.expectedErr) {
					t.Fatalf("Import error = '%v', expected '%v'", err, expectedResult.expectedErr)
				}
			})
		}
	}
}
