package committed_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/committed/mock"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

type actionType string

const (
	actionTypeWriteRange  actionType = "write-range"
	actionTypeWriteRecord actionType = "write-record"
)

type writeAction struct {
	action   actionType
	rng      committed.Range
	key      string
	identity string
}

type recordMatcher struct {
	key      graveler.Key
	identity []byte
}

func newRecordMatcher(key string, identity string) recordMatcher {
	return recordMatcher{
		key:      graveler.Key(key),
		identity: []byte(identity),
	}
}

func (m recordMatcher) Matches(x interface{}) bool {
	record := x.(graveler.ValueRecord)
	return bytes.Equal(m.key, record.Key) && bytes.Equal(m.identity, record.Identity)
}

func (m recordMatcher) String() string {
	return fmt.Sprintf("is equal to key:%s, identity:%s", m.key, m.identity)
}

type testValueRecord struct {
	key      string
	identity string
}

type testRange struct {
	rng     committed.Range
	records []testValueRecord
}

type testRunResult struct {
	mergeStrategies []graveler.MergeStrategy
	expectedActions []writeAction
	expectedErr     error
}

type testMetaRange struct {
	ranges []testRange
}

func (t *testMetaRange) GetMetaRangeID() graveler.MetaRangeID {
	var sb strings.Builder
	for _, rng := range t.ranges {
		sb.WriteString(string(rng.rng.ID))
		sb.WriteString("#")
	}
	return graveler.MetaRangeID(sb.String())
}

func newTestMetaRange(ranges []testRange) *testMetaRange {
	return &testMetaRange{ranges: ranges}
}

type testCase struct {
	baseRange      *testMetaRange
	sourceRange    *testMetaRange
	destRange      *testMetaRange
	expectedResult []testRunResult
}

type testCases map[string]testCase

func createIter(tr *testMetaRange) committed.Iterator {
	iter := testutil.NewFakeIterator()
	for _, rng := range tr.ranges {
		addRange := rng.rng
		iter.AddRange(&addRange)
		for _, record := range rng.records {
			iter.AddValueRecords(&graveler.ValueRecord{
				Key: graveler.Key(record.key),
				Value: &graveler.Value{
					Identity: []byte(record.identity),
					Data:     nil,
				},
			})
		}
	}
	return iter
}

func Test_merge(t *testing.T) {
	tests := testCases{
		"dest range added before": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
					records: nil,
				}, {rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
				records: nil,
			}}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "dest:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "dest:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234},
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
					},
				},
				expectedErr: nil,
			}},
		},
		"source range added before": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
				{rng: committed.Range{ID: "base:k7-k8", MinKey: committed.Key("k7"), MaxKey: committed.Key("k8"), Count: 2, EstimatedSize: 1024}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "source:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1024}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "source:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1024},
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024},
					},
				},
			}},
		},
		"source range removed before": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1024}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1024}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024},
					},
				},
			}},
		},
		"source range inner change": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"},
				}},
				{rng: committed.Range{ID: "base:k4", MinKey: committed.Key("k4"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k4", "base:k4"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "source:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k2", "source:k2"}, {"k3", "source:k3"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "source:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024},
					},
				},
			}},
		},
		"dest range inner change": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "dest:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k2", "dest:k2"},
					{"k3", "dest:k3"},
				}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "dest:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024},
					},
				},
				expectedErr: nil,
			}},
		},
		"source range append after": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), Count: 2, EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "source:k1-k5", MinKey: committed.Key("k1"), MaxKey: committed.Key("k5"), Count: 4, EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
					{"k4", "source:k4"},
					{"k5", "source:k5"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), Count: 2, EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
			}),
			expectedResult: []testRunResult{
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "k1",
							identity: "base:k1",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "k3",
							identity: "base:k3",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "k4",
							identity: "source:k4",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "k5",
							identity: "source:k5",
						},
					},
				},
			},
		},
		"source range append and remove after": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3")}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"},
				}},
				{
					rng: committed.Range{ID: "base:k4-k6", MinKey: committed.Key("k4"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
						{"k4", "base:k4"}, {"k6", "base:k6"},
					},
				},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "source:k1-k5", MinKey: committed.Key("k1"), MaxKey: committed.Key("k5")}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k2", "source:k2"}, {"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "source:k5"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3")}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"},
				}},
				{
					rng: committed.Range{ID: "base:k4-k6", MinKey: committed.Key("k4"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
						{"k4", "base:k4"}, {"k6", "base:k6"},
					},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "k1",
						identity: "base:k1",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "k2",
						identity: "source:k2",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "k3",
						identity: "base:k3",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "k4",
						identity: "base:k4",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "k5",
						identity: "source:k5",
					},
				},
				expectedErr: nil,
			}},
		},
		"source range - overlapping ranges": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "source:k1-k10", MinKey: committed.Key("k1"), MaxKey: committed.Key("k10"), Count: 6, EstimatedSize: 66666}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k4", "source:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"}, {"k10", "source:k10"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{action: actionTypeWriteRecord, key: "k1", identity: "base:k1"},
					{action: actionTypeWriteRecord, key: "k3", identity: "base:k3"},
					{action: actionTypeWriteRecord, key: "k4", identity: "source:k4"},
					{action: actionTypeWriteRecord, key: "k5", identity: "base:k5"},
					{action: actionTypeWriteRecord, key: "k6", identity: "base:k6"},
					{action: actionTypeWriteRecord, key: "k10", identity: "source:k10"},
				},
			}},
		},
		"dest range - overlapping ranges": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "dest:k1-k10", MinKey: committed.Key("k1"), MaxKey: committed.Key("k10"), Count: 6, EstimatedSize: 66666}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k4", "dest:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"}, {"k10", "dest:k10"},
				}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{action: actionTypeWriteRecord, key: "k1", identity: "base:k1"},
					{action: actionTypeWriteRecord, key: "k3", identity: "base:k3"},
					{action: actionTypeWriteRecord, key: "k4", identity: "dest:k4"},
					{action: actionTypeWriteRecord, key: "k5", identity: "base:k5"},
					{action: actionTypeWriteRecord, key: "k6", identity: "base:k6"},
					{action: actionTypeWriteRecord, key: "k10", identity: "dest:k10"},
				},
				expectedErr: nil,
			}},
		},
		"source - remove at end of range": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "dest:k1-k10", MinKey: committed.Key("k1"), MaxKey: committed.Key("k10"), Count: 6, EstimatedSize: 66666}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k4", "dest:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"}, {"k10", "dest:k10"},
				}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{action: actionTypeWriteRecord, key: "k1", identity: "base:k1"},
					{action: actionTypeWriteRecord, key: "k3", identity: "base:k3"},
					{action: actionTypeWriteRecord, key: "k4", identity: "dest:k4"},
					{action: actionTypeWriteRecord, key: "k5", identity: "base:k5"},
					{action: actionTypeWriteRecord, key: "k6", identity: "base:k6"},
					{action: actionTypeWriteRecord, key: "k10", identity: "dest:k10"},
				},
				expectedErr: nil,
			}},
		},
		"both added key to range": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k6", "base:k6"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "source:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k2", "source:k2"},
					{"k6", "base:k6"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "dest:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "dest:k3"},
					{"k6", "base:k6"},
				}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "k1",
						identity: "base:k1",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "k2",
						identity: "source:k2",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "k3",
						identity: "dest:k3",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "k6",
						identity: "base:k6",
					},
				},
			}},
		},
		"source range removed": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
				records: nil,
			}}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
					},
				},
				expectedErr: nil,
			}},
		},
		"dest range removed": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
					},
				},
				expectedErr: nil,
			}},
		},
		"source key removed from range - same bounds": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{
					rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{
					rng: committed.Range{ID: "source:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k6", "base:k6"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{
					rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{action: actionTypeWriteRange, rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
					{action: actionTypeWriteRange, rng: committed.Range{ID: "source:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
				},
			}},
		},
		"source key removed from range": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{
					rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{
					rng: committed.Range{ID: "source:k3-k5", MinKey: committed.Key("k3"), MaxKey: committed.Key("k5"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k5", "base:k5"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{
					rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{action: actionTypeWriteRange, rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
					{action: actionTypeWriteRecord, key: "k3", identity: "base:k3"},
					{action: actionTypeWriteRecord, key: "k5", identity: "base:k5"},
				},
			}},
		},
		"dest key removed from range": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{
					rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{
					rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{
					rng: committed.Range{ID: "dest:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k6", "base:k6"},
					},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{action: actionTypeWriteRange, rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
					{action: actionTypeWriteRange, rng: committed.Range{ID: "dest:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
				},
				expectedErr: nil,
			}},
		},
		"empty source and base": {
			baseRange:   newTestMetaRange([]testRange{}),
			sourceRange: newTestMetaRange([]testRange{}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "dest:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{},
				expectedErr:     graveler.ErrNoChanges,
			}},
		},
		"dest removed range and added range after source removed range edges": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "base:k1-k5", MinKey: committed.Key("k1"), MaxKey: committed.Key("k5"), Count: 5, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k1", "base:k1"},
						{"k2", "base:k2"},
						{"k3", "base:k3"},
						{"k4", "base:k4"},
						{"k5", "base:k5"},
					},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "source:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "dest:k6-k7", MinKey: committed.Key("k6"), MaxKey: committed.Key("k7"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k6", "dest:k6"}, {"k7", "dest:k7"},
					},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{action: actionTypeWriteRecord, key: "k6", identity: "dest:k6"},
					{action: actionTypeWriteRecord, key: "k7", identity: "dest:k7"},
				},
				expectedErr: nil,
			}},
		},
		"no changes": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "base:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"},
					},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "base:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "base:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"},
					},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{},
				expectedErr:     graveler.ErrNoChanges,
			}},
		},
		"source and dest changed record identity": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:b-c", MinKey: committed.Key("b"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}, {key: "c", identity: "c"}},
				},
				{
					rng:     committed.Range{ID: "base:d-e", MinKey: committed.Key("d"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "d", identity: "d"}, {key: "e", identity: "e"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "d", identity: "d"}},
				},
				{
					rng:     committed.Range{ID: "source:e", MinKey: committed.Key("e"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "e", identity: "e1"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:b-c", MinKey: committed.Key("b"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}, {key: "c", identity: "c"}},
				},
				{
					rng:     committed.Range{ID: "dest:d-e", MinKey: committed.Key("d"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "d", identity: "d1"}, {key: "e", identity: "e"}},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "a",
						identity: "a",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "b",
						identity: "b",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "d",
						identity: "d1",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "e",
						identity: "e1",
					},
				},
				expectedErr: nil,
			}},
		},
		"dest removed all source added": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 4, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "b",
						identity: "b",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "c",
						identity: "c",
					},
				},
				expectedErr: nil,
			}},
		},
		"same identity different key": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 4, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "a1", identity: "a"}, {key: "c", identity: "c"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "dest:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "a",
						identity: "a",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "a1",
						identity: "a",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "c",
						identity: "c",
					},
				},
				expectedErr: nil,
			}},
		},
		"dest removed all source range before base": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:c-d", MinKey: committed.Key("c"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "source:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					},
				},
				expectedErr: nil,
			}},
		},
		"dest removed all different key different identity": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "a2", identity: "a2"}, {key: "b", identity: "b"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "a1", identity: "a1"}, {key: "b", identity: "b"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "a1",
						identity: "a1",
					},
				},
				expectedErr: nil,
			}},
		},
		"dest removed all base and source same identity": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
				{
					rng:     committed.Range{ID: "base:d-f", MinKey: committed.Key("d"), MaxKey: committed.Key("f"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "d", identity: "d"}, {key: "f", identity: "f"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
				{
					rng:     committed.Range{ID: "source:c-e", MinKey: committed.Key("c"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}, {key: "e", identity: "e"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "c",
						identity: "c",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "e",
						identity: "e",
					},
				},
				expectedErr: nil,
			}},
		},
		"source key before dest range": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "dest:b-c", MinKey: committed.Key("b"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}, {key: "c", identity: "c"}},
				},
				{
					rng:     committed.Range{ID: "dest:e-f", MinKey: committed.Key("e"), MaxKey: committed.Key("f"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "e", identity: "e"}, {key: "f", identity: "f"}},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "b",
						identity: "b",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "c",
						identity: "c",
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "dest:e-f", MinKey: committed.Key("e"), MaxKey: committed.Key("f"), Count: 2, EstimatedSize: 1024},
					},
				},
				expectedErr: nil,
			}},
		},
		"dest key before source range": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:b-c", MinKey: committed.Key("b"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}, {key: "c", identity: "c"}},
				},
				{
					rng:     committed.Range{ID: "source:e-f", MinKey: committed.Key("e"), MaxKey: committed.Key("f"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "e", identity: "e"}, {key: "f", identity: "f"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "dest:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "b",
						identity: "b",
					},
					{
						action:   actionTypeWriteRecord,
						key:      "c",
						identity: "c",
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "source:e-f", MinKey: committed.Key("e"), MaxKey: committed.Key("f"), Count: 2, EstimatedSize: 1024},
					},
				},
				expectedErr: nil,
			}},
		},
		"dest range before source key": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:c-c", MinKey: committed.Key("c"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "dest:a-a", MinKey: committed.Key("a"), MaxKey: committed.Key("a"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}},
				},
				{
					rng:     committed.Range{ID: "dest:b-b", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}},
				},
				{
					rng:     committed.Range{ID: "base:c-c", MinKey: committed.Key("c"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "a",
						identity: "a",
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "dest:b-b", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					},
					{
						action:   actionTypeWriteRecord,
						key:      "d",
						identity: "d",
					},
				},
				expectedErr: nil,
			}},
		},
		"source range before dest key": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:c-c", MinKey: committed.Key("c"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:a-a", MinKey: committed.Key("a"), MaxKey: committed.Key("a"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}},
				},
				{
					rng:     committed.Range{ID: "source:b-b", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}},
				},
				{
					rng:     committed.Range{ID: "base:c-c", MinKey: committed.Key("c"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "dest:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{
					{
						action:   actionTypeWriteRecord,
						key:      "a",
						identity: "a",
					},
					{
						action: actionTypeWriteRange,
						rng:    committed.Range{ID: "source:b-b", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					},
					{
						action:   actionTypeWriteRecord,
						key:      "d",
						identity: "d",
					},
				},
				expectedErr: nil,
			}},
		},
		"dest and base are the same": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "source:k13-k14", MinKey: committed.Key("k13"), MaxKey: committed.Key("k14"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k13", "base:k13"}, {"k14", "base:k14"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{},
			}},
		},
		"source and dest are the same": {
			baseRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			destRange: newTestMetaRange([]testRange{
				{rng: committed.Range{ID: "base:k11-k12", MinKey: committed.Key("k11"), MaxKey: committed.Key("k12"), Count: 2, EstimatedSize: 4444}, records: []testValueRecord{
					{"k11", "base:k11"}, {"k12", "base:k12"},
				}},
			}),
			expectedResult: []testRunResult{{
				mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone, graveler.MergeStrategyDest, graveler.MergeStrategySrc},
				expectedActions: []writeAction{},
				expectedErr:     graveler.ErrNoChanges,
			}},
		},
	}

	runMergeTests(tests, t)
}

func TestMergeStrategies(t *testing.T) {
	tests := testCases{
		// Base branch has a 2 records range, with the keys 'a' and 'b'. Source branch changes the value on key 'a' and leaves key 'b' unchanged
		// Dest branch deletes both entries, creating a conflict on entry 'a'
		// As per merge strategies, the expected outcomes are:
		// - No strategy - a conflict
		// - Dest strategy - favors dest branch, so both records are deleted, 'ignoring' the value modification on record 'a'
		// - Source strategy - favors source branch, so record 'a', with the modified value, is written, ignoring its deletion on dest. Record b
		//   is still deleted as there is no conflict involving it
		"dest removed all same key different identity": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a1"}, {key: "b", identity: "b"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{}),
			expectedResult: []testRunResult{
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone},
					expectedActions: []writeAction{},
					expectedErr:     graveler.ErrConflictFound,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyDest},
					expectedActions: []writeAction{},
					expectedErr:     nil,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategySrc},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a1",
						},
					},
					expectedErr: nil,
				},
			},
		},
		// Base branch has a 5 records range, with the keys 'k1' through 'k5'. Both source and dest branches modify this range by deleting and
		// changing this range. Both branches delete records 'k1', 'k2' and 'k5' and while dest branch also deletes entries 'k3' and 'k4', source
		// branch leaves record 'k3' unchanged and changes the value on record 'k4', creating a conflict
		// Dest branch also adds 2 new records, 'k6' and 'k7', which should not create a conflict
		// As per merge strategies, the expected outcomes are:
		// - No strategy - a conflict
		// - Dest strategy - favors dest branch, so records 'k1' through 'k5' are deleted, 'ignoring' the value modification on record 'k4'.
		// - Source strategy - favors source branch, so record 'k4', with the modified value, is written, ignoring its deletion on dest. The rest
		//   of record 'k1', 'k2' 'k3' and 'k5' are still deleted as there is no conflict involving them. Same goes for 'k6' and 'k7' which are
		//   written on both 'dest-wins' and 'source-wins' strategies
		"source and dest change same range conflict": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "base:k1-k5", MinKey: committed.Key("k1"), MaxKey: committed.Key("k5"), Count: 5, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k1", "base:k1"},
						{"k2", "base:k2"},
						{"k3", "base:k3"},
						{"k4", "base:k4"},
						{"k5", "base:k5"},
					},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "source:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "source:k4"},
					},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng: committed.Range{ID: "dest:k6-k7", MinKey: committed.Key("k6"), MaxKey: committed.Key("k7"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k6", "dest:k6"}, {"k7", "dest:k7"},
					},
				},
			}),
			expectedResult: []testRunResult{
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone},
					expectedActions: []writeAction{},
					expectedErr:     graveler.ErrConflictFound,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyDest},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "k6",
							identity: "dest:k6",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "k7",
							identity: "dest:k7",
						},
					},
					expectedErr: nil,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategySrc},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "k4",
							identity: "source:k4",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "k6",
							identity: "dest:k6",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "k7",
							identity: "dest:k7",
						},
					},
					expectedErr: nil,
				},
			},
		},
		// Base branch has a 2 records range, with the keys 'a' and 'b'. Source branch deletes both records and adds 2 new records - 'c' and 'd'
		// Dest branch changes the value on key 'a', creating a conflict with the deleted 'a' record on source, and leaves key 'b' unchanged
		// As per merge strategies, the expected outcomes are:
		// - No strategy - a conflict
		// - Dest strategy - favors dest branch, so record 'a', with the modified value, is written, ignoring its deletion on source
		// - Source strategy - favors source branch, so record 'a' is deleted, ignoring its value change on branch dest
		// Record 'b' is deleted for both strategies as there is no conflict involving it. Same goes for records 'c' and 'd' that are written,
		// regardless of merge strategy
		"dest range before source": {
			baseRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
			}),
			sourceRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "source:c-d", MinKey: committed.Key("c"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			}),
			destRange: newTestMetaRange([]testRange{
				{
					rng:     committed.Range{ID: "dest:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a1"}, {key: "b", identity: "b"}},
				},
			}),
			expectedResult: []testRunResult{
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone},
					expectedActions: []writeAction{},
					expectedErr:     graveler.ErrConflictFound,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyDest},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a1",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "c",
							identity: "c",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "d",
							identity: "d",
						},
					},
					expectedErr: nil,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategySrc},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "c",
							identity: "c",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "d",
							identity: "d",
						},
					},
					expectedErr: nil,
				},
			},
		},
		"source change and dest delete same entry": {
			baseRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "base:a-c", MinKey: committed.Key("a"), MaxKey: committed.Key("c"), Count: 3, EstimatedSize: 333},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}},
			}}),
			sourceRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "source:a-c", MinKey: committed.Key("a"), MaxKey: committed.Key("c"), Count: 3, EstimatedSize: 123},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b1"}, {key: "c", identity: "c"}},
			}}),
			destRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "dest:a-c", MinKey: committed.Key("a"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 321},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "c", identity: "c"}},
			}}),
			expectedResult: []testRunResult{
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
					},
					expectedErr: graveler.ErrConflictFound,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyDest},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "c",
							identity: "c",
						},
					},
					expectedErr: nil,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategySrc},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "b",
							identity: "b1",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "c",
							identity: "c",
						},
					},
					expectedErr: nil,
				},
			},
		},

		"source delete and dest change same entry": {
			baseRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "base:a-c", MinKey: committed.Key("a"), MaxKey: committed.Key("c"), Count: 3, EstimatedSize: 333},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}},
			}}),
			sourceRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "source:a-c", MinKey: committed.Key("a"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 123},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "c", identity: "c"}},
			}}),
			destRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "dest:a-c", MinKey: committed.Key("a"), MaxKey: committed.Key("c"), Count: 3, EstimatedSize: 321},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b1"}, {key: "c", identity: "c"}},
			}}),
			expectedResult: []testRunResult{
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
					},
					expectedErr: graveler.ErrConflictFound,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyDest},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "b",
							identity: "b1",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "c",
							identity: "c",
						},
					},
					expectedErr: nil,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategySrc},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "c",
							identity: "c",
						},
					},
					expectedErr: nil,
				},
			},
		},

		"source and dest delete from same range": {
			baseRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "base:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 4, EstimatedSize: 4444},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
			}}),
			sourceRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 3, EstimatedSize: 1234},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "c", identity: "c1"}, {key: "d", identity: "d"}},
			}}),
			destRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "dest:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 3, EstimatedSize: 4321},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b1"}, {key: "d", identity: "d"}},
			}}),
			expectedResult: []testRunResult{
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
					},
					expectedErr: graveler.ErrConflictFound,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyDest},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "b",
							identity: "b1",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "d",
							identity: "d",
						},
					},
					expectedErr: nil,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategySrc},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "c",
							identity: "c1",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "d",
							identity: "d",
						},
					},
					expectedErr: nil,
				},
			},
		},
		"source and dest change same entry": {
			baseRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "base:a-a", MinKey: committed.Key("a"), MaxKey: committed.Key("c"), Count: 3, EstimatedSize: 1234},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}},
			}}),
			sourceRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "source:a-a", MinKey: committed.Key("a"), MaxKey: committed.Key("c"), Count: 3, EstimatedSize: 1234},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b1"}, {key: "c", identity: "c"}},
			}}),
			destRange: newTestMetaRange([]testRange{{
				rng:     committed.Range{ID: "dest:a-a", MinKey: committed.Key("a"), MaxKey: committed.Key("c"), Count: 3, EstimatedSize: 1234},
				records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b2"}, {key: "c", identity: "c"}},
			}}),
			expectedResult: []testRunResult{
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyNone},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
					},
					expectedErr: graveler.ErrConflictFound,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategyDest},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "b",
							identity: "b2",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "c",
							identity: "c",
						},
					},
					expectedErr: nil,
				},
				{
					mergeStrategies: []graveler.MergeStrategy{graveler.MergeStrategySrc},
					expectedActions: []writeAction{
						{
							action:   actionTypeWriteRecord,
							key:      "a",
							identity: "a",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "b",
							identity: "b1",
						},
						{
							action:   actionTypeWriteRecord,
							key:      "c",
							identity: "c",
						},
					},
					expectedErr: nil,
				},
			},
		},
	}

	runMergeTests(tests, t)
}

func runMergeTests(tests testCases, t *testing.T) {
	for name, tst := range tests {
		for _, expectedResult := range tst.expectedResult {
			for _, mergeStrategy := range expectedResult.mergeStrategies {
				t.Run(name, func(t *testing.T) {
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
					metaRangeManager.EXPECT().NewWriter(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(writer)
					sourceMetaRangeID := tst.sourceRange.GetMetaRangeID()
					destMetaRangeID := tst.destRange.GetMetaRangeID()
					baseMetaRangeID := tst.baseRange.GetMetaRangeID()
					metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), baseMetaRangeID).AnyTimes().Return(createIter(tst.baseRange), nil)
					metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), sourceMetaRangeID).AnyTimes().Return(createIter(tst.sourceRange), nil)
					metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), destMetaRangeID).AnyTimes().Return(createIter(tst.destRange), nil)

					rangeManager := mock.NewMockRangeManager(ctrl)

					writer.EXPECT().Abort().AnyTimes()
					metaRangeId := graveler.MetaRangeID("merge")
					writer.EXPECT().Close().Return(&metaRangeId, nil).AnyTimes()
					committedManager := committed.NewCommittedManager(metaRangeManager, rangeManager, params)
					_, err := committedManager.Merge(ctx, "ns", destMetaRangeID, sourceMetaRangeID, baseMetaRangeID, mergeStrategy)
					if err != expectedResult.expectedErr {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestMergeCancelContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("source", func(t *testing.T) {
		base := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 1}).
			AddValueRecords(makeV("b", "dest:b"))
		source := testutil.NewFakeIterator()
		destination := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 1}).
			AddValueRecords(makeV("b", "dest:b"))
		writer := mock.NewMockMetaRangeWriter(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := committed.Merge(ctx, writer, base, source, destination, graveler.MergeStrategyNone)
		assert.True(t, errors.Is(err, context.Canceled), "context canceled error")
	})

	t.Run("destination", func(t *testing.T) {
		base := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 2}).
			AddValueRecords(makeV("a", "base:a"), makeV("c", "base:c"))
		source := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 2}).
			AddValueRecords(makeV("a", "base:a"), makeV("c", "base:c"))
		destination := testutil.NewFakeIterator()
		writer := mock.NewMockMetaRangeWriter(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := committed.Merge(ctx, writer, base, source, destination, graveler.MergeStrategyNone)
		assert.True(t, errors.Is(err, context.Canceled), "context canceled error")
	})

	t.Run("source_and_destination", func(t *testing.T) {
		base := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
			AddValueRecords(makeV("a", "base:a"), makeV("c", "base:c"))
		source := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "two", MinKey: committed.Key("e"), MaxKey: committed.Key("f"), Count: 2}).
			AddValueRecords(makeV("e", "source:e"), makeV("f", "source:f"))
		destination := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "three", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 1}).
			AddValueRecords(makeV("b", "dest:b"))
		writer := mock.NewMockMetaRangeWriter(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := committed.Merge(ctx, writer, base, source, destination, graveler.MergeStrategyNone)
		assert.True(t, errors.Is(err, context.Canceled), "context canceled error")
	})

	t.Run("base", func(t *testing.T) {
		base := testutil.NewFakeIterator()
		source := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "two", MinKey: committed.Key("e"), MaxKey: committed.Key("f"), Count: 2}).
			AddValueRecords(makeV("e", "source:e"), makeV("f", "source:f"))
		destination := testutil.NewFakeIterator().
			AddRange(&committed.Range{ID: "one", MinKey: committed.Key("b"), MaxKey: committed.Key("b"), Count: 1}).
			AddValueRecords(makeV("b", "dest:b"))
		writer := mock.NewMockMetaRangeWriter(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := committed.Merge(ctx, writer, base, source, destination, graveler.MergeStrategyNone)
		assert.True(t, errors.Is(err, context.Canceled), "context canceled error")
	})
}
