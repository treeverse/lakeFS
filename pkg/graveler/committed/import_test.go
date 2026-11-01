package committed_test

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/committed/mock"
)

// Tests for graveler.CommittedManager.Import
func Test_import(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		sourceRange    *testMetaRange
		destRange      *testMetaRange
		storageID      graveler.StorageID
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
			storageID: config.SingleBlockstoreID,
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
			name: "source range smaller than dest range, alternate StorageID",
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
			storageID: "alternate_storage",
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
			storageID: config.SingleBlockstoreID,
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
			storageID: config.SingleBlockstoreID,
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
			storageID: config.SingleBlockstoreID,
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
			storageID: config.SingleBlockstoreID,
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
			storageID: config.SingleBlockstoreID,
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
		{
			// #10572: once the source is exhausted, a dest range that straddles the prefix must be
			// filtered record by record (WriteRecord), not copied whole (WriteRange).
			name: "source exhausted before dest range straddling prefix",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "p/f-p/f", MinKey: committed.Key("p/f"), MaxKey: committed.Key("p/f"), Count: 1, EstimatedSize: 512},
					records: []testValueRecord{{"p/f", "pf"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"a/1", "a1"}, {"a/2", "a2"}},
				},
				{
					rng:     committed.Range{ID: "p/old/1-q", MinKey: committed.Key("p/old/1"), MaxKey: committed.Key("q"), Count: 3, EstimatedSize: 1536},
					records: []testValueRecord{{"p/old/1", "pold1"}, {"p/old/2", "pold2"}, {"q", "q"}},
				},
			}),
			storageID: config.SingleBlockstoreID,
			prefixes:  []graveler.Prefix{"p/"},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{action: actionTypeWriteRange, rng: committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024}},
					{action: actionTypeWriteRange, rng: committed.Range{ID: "p/f-p/f", MinKey: committed.Key("p/f"), MaxKey: committed.Key("p/f"), Count: 1, EstimatedSize: 512}},
					{action: actionTypeWriteRecord, key: "q", identity: "q"},
				},
			}},
		},
		{
			// #10572 issue shape: the bounded and right-straddling ranges are both reached after the
			// source is exhausted; neither may be copied whole.
			name: "small source before left-straddling, bounded and right-straddling dest ranges",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "p/f-p/f", MinKey: committed.Key("p/f"), MaxKey: committed.Key("p/f"), Count: 1, EstimatedSize: 512},
					records: []testValueRecord{{"p/f", "pf"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "a-p/old/1", MinKey: committed.Key("a"), MaxKey: committed.Key("p/old/1"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"a", "a"}, {"p/old/1", "pold1"}},
				},
				{
					rng:     committed.Range{ID: "p/old/2-p/old/3", MinKey: committed.Key("p/old/2"), MaxKey: committed.Key("p/old/3"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"p/old/2", "pold2"}, {"p/old/3", "pold3"}},
				},
				{
					rng:     committed.Range{ID: "p/old/4-q", MinKey: committed.Key("p/old/4"), MaxKey: committed.Key("q"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"p/old/4", "pold4"}, {"q", "q"}},
				},
			}),
			storageID: config.SingleBlockstoreID,
			prefixes:  []graveler.Prefix{"p/"},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{action: actionTypeWriteRecord, key: "a", identity: "a"},
					{action: actionTypeWriteRecord, key: "p/f", identity: "pf"},
					{action: actionTypeWriteRecord, key: "q", identity: "q"},
				},
			}},
		},
		{
			// Re-import of an empty directory: every dest range goes through handleAll. Ranges outside
			// the prefix are still copied whole; the straddling one is filtered.
			name:        "empty source",
			sourceRange: newTestMetaRange(nil),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"a/1", "a1"}, {"a/2", "a2"}},
				},
				{
					rng:     committed.Range{ID: "b-q", MinKey: committed.Key("b"), MaxKey: committed.Key("q"), Count: 3, EstimatedSize: 1536},
					records: []testValueRecord{{"b", "b"}, {"p/1", "p1"}, {"q", "q"}},
				},
				{
					rng:     committed.Range{ID: "r-s", MinKey: committed.Key("r"), MaxKey: committed.Key("s"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"r", "r"}, {"s", "s"}},
				},
			}),
			storageID: config.SingleBlockstoreID,
			prefixes:  []graveler.Prefix{"p/"},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{action: actionTypeWriteRange, rng: committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024}},
					{action: actionTypeWriteRecord, key: "b", identity: "b"},
					{action: actionTypeWriteRecord, key: "q", identity: "q"},
					{action: actionTypeWriteRange, rng: committed.Range{ID: "r-s", MinKey: committed.Key("r"), MaxKey: committed.Key("s"), Count: 2, EstimatedSize: 1024}},
				},
			}},
		},
		{
			// Prefix equal to a range MaxKey ("c/") and to a range MinKey ("p/"); keys equal to the
			// prefix string are removed too. The range between the prefixes is copied whole.
			name:        "prefixes on range bounds with empty source",
			sourceRange: newTestMetaRange(nil),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "a-c/", MinKey: committed.Key("a"), MaxKey: committed.Key("c/"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"a", "a"}, {"c/", "c"}},
				},
				{
					rng:     committed.Range{ID: "d-e", MinKey: committed.Key("d"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"d", "d"}, {"e", "e"}},
				},
				{
					rng:     committed.Range{ID: "p/-q", MinKey: committed.Key("p/"), MaxKey: committed.Key("q"), Count: 3, EstimatedSize: 1536},
					records: []testValueRecord{{"p/", "p"}, {"p/x", "px"}, {"q", "q"}},
				},
			}),
			storageID: config.SingleBlockstoreID,
			prefixes:  []graveler.Prefix{"c/", "p/"},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{action: actionTypeWriteRecord, key: "a", identity: "a"},
					{action: actionTypeWriteRange, rng: committed.Range{ID: "d-e", MinKey: committed.Key("d"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024}},
					{action: actionTypeWriteRecord, key: "q", identity: "q"},
				},
			}},
		},
		{
			// Whole-branch import: every dest range is bounded by "" and skipped; the source is copied whole.
			name: "empty string prefix replaces whole branch",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "p/f-p/f", MinKey: committed.Key("p/f"), MaxKey: committed.Key("p/f"), Count: 1, EstimatedSize: 512},
					records: []testValueRecord{{"p/f", "pf"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"a/1", "a1"}, {"a/2", "a2"}},
				},
				{
					rng:     committed.Range{ID: "p/old/1-q", MinKey: committed.Key("p/old/1"), MaxKey: committed.Key("q"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"p/old/1", "pold1"}, {"q", "q"}},
				},
			}),
			storageID: config.SingleBlockstoreID,
			prefixes:  []graveler.Prefix{""},
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{action: actionTypeWriteRange, rng: committed.Range{ID: "p/f-p/f", MinKey: committed.Key("p/f"), MaxKey: committed.Key("p/f"), Count: 1, EstimatedSize: 512}},
				},
			}},
		},
		{
			// No prefixes: dest ranges left after the source is exhausted are still copied whole.
			name: "no prefixes keeps whole-range copy after source exhausted",
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "p/f-p/f", MinKey: committed.Key("p/f"), MaxKey: committed.Key("p/f"), Count: 1, EstimatedSize: 512},
					records: []testValueRecord{{"p/f", "pf"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"a/1", "a1"}, {"a/2", "a2"}},
				},
				{
					rng:     committed.Range{ID: "p/old/1-q", MinKey: committed.Key("p/old/1"), MaxKey: committed.Key("q"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{"p/old/1", "pold1"}, {"q", "q"}},
				},
			}),
			storageID: config.SingleBlockstoreID,
			prefixes:  nil,
			expectedResult: []testRunResult{{
				expectedActions: []writeAction{
					{action: actionTypeWriteRange, rng: committed.Range{ID: "a/1-a/2", MinKey: committed.Key("a/1"), MaxKey: committed.Key("a/2"), Count: 2, EstimatedSize: 1024}},
					{action: actionTypeWriteRange, rng: committed.Range{ID: "p/f-p/f", MinKey: committed.Key("p/f"), MaxKey: committed.Key("p/f"), Count: 1, EstimatedSize: 512}},
					{action: actionTypeWriteRange, rng: committed.Range{ID: "p/old/1-q", MinKey: committed.Key("p/old/1"), MaxKey: committed.Key("q"), Count: 2, EstimatedSize: 1024}},
				},
			}},
		},
	}

	for _, tst := range tests {
		for _, expectedResult := range tst.expectedResult {
			t.Run(tst.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				ctx := t.Context()
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
				metaRangeManager.EXPECT().NewWriter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(writer)
				sourceMetaRangeID := tst.sourceRange.GetMetaRangeID()
				destMetaRangeID := tst.destRange.GetMetaRangeID()
				metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), graveler.MetaRangeID("")).AnyTimes().Return(committed.NewEmptyIterator(), nil) // empty base
				metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), sourceMetaRangeID).AnyTimes().Return(createIter(tst.sourceRange), nil)
				metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), destMetaRangeID).AnyTimes().Return(createIter(tst.destRange), nil)

				rangeManager := mock.NewMockRangeManager(ctrl)

				writer.EXPECT().Abort().AnyTimes()
				metaRangeId := graveler.MetaRangeID("import")
				writer.EXPECT().Close(gomock.Any()).Return(&metaRangeId, nil).AnyTimes()

				rangeManagers := make(map[graveler.StorageID]committed.RangeManager)
				rangeManagers[tst.storageID] = rangeManager
				metaRangeManagers := make(map[graveler.StorageID]committed.MetaRangeManager)
				metaRangeManagers[tst.storageID] = metaRangeManager
				committedManager := committed.NewCommittedManager(metaRangeManagers, rangeManagers, nil, params)
				_, err := committedManager.Import(ctx, tst.storageID, "ns", destMetaRangeID, sourceMetaRangeID, tst.prefixes)
				if !errors.Is(err, expectedResult.expectedErr) {
					t.Fatalf("Import error = '%v', expected '%v'", err, expectedResult.expectedErr)
				}
			})
		}
	}
}
