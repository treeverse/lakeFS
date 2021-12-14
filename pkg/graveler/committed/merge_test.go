package committed_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/go-test/deep"
	"github.com/golang/mock/gomock"
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
	matcher  gomock.Matcher
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

func createIter(tr []testRange) committed.Iterator {
	iter := testutil.NewFakeIterator()
	for _, rng := range tr {
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
	tests := map[string]struct {
		baseRange           []testRange
		sourceRange         []testRange
		destRange           []testRange
		expectedActions     []writeAction
		conflictExpectedIdx *int
		expectedSummary     graveler.DiffSummary
		expectedErr         error
	}{
		"dest range added before": {
			baseRange: []testRange{{
				rng:     committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
				records: nil,
			}},
			sourceRange: []testRange{{
				rng:     committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
				records: nil,
			}},
			destRange: []testRange{
				{rng: committed.Range{ID: "dest:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			},
			conflictExpectedIdx: nil,
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
			expectedSummary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
			expectedErr:     graveler.ErrNoChanges,
		},
		"source range added before": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "source:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1024}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			},
			conflictExpectedIdx: nil,
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
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{added: 2}},
		},
		"source range removed before": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1024}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1024}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024}},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{
					action: actionTypeWriteRange,
					rng:    committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1024},
				},
			},
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{removed: 2}},
		},
		"source range inner change": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"},
				}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "source:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k2", "source:k2"}, {"k3", "source:k3"},
				}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{
					action: actionTypeWriteRange,
					rng:    committed.Range{ID: "source:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024},
				},
			},
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{}, Incomplete: true},
		},
		"dest range inner change": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "dest:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k2", "dest:k2"},
					{"k3", "dest:k3"},
				}},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{
					action: actionTypeWriteRange,
					rng:    committed.Range{ID: "dest:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), EstimatedSize: 1024},
				},
			},
			expectedErr:     graveler.ErrNoChanges,
			expectedSummary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
		},
		"source range append after": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), Count: 2, EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "source:k1-k5", MinKey: committed.Key("k1"), MaxKey: committed.Key("k5"), Count: 4, EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
					{"k4", "source:k4"},
					{"k5", "source:k5"},
				}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), Count: 2, EstimatedSize: 1024}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "base:k3"},
				}},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{
					action: actionTypeWriteRange,
					rng:    committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3"), Count: 2, EstimatedSize: 1024},
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
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{added: 2}},
		},
		"source range append and remove after": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3")}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"},
				}},
				{rng: committed.Range{ID: "base:k4-k6", MinKey: committed.Key("k4"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
					{"k4", "base:k4"}, {"k6", "base:k6"}},
				}},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "source:k1-k5", MinKey: committed.Key("k1"), MaxKey: committed.Key("k5")}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k2", "source:k2"}, {"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "source:k5"},
				}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k3", MinKey: committed.Key("k1"), MaxKey: committed.Key("k3")}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"},
				}},
				{rng: committed.Range{ID: "base:k4-k6", MinKey: committed.Key("k4"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
					{"k4", "base:k4"}, {"k6", "base:k6"}},
				}},
			conflictExpectedIdx: nil,
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
			expectedErr:     nil,
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{added: 2, removed: 1}},
		},
		"source range - overlapping ranges": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "source:k1-k10", MinKey: committed.Key("k1"), MaxKey: committed.Key("k10"), Count: 6, EstimatedSize: 66666}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k4", "source:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"}, {"k10", "source:k10"},
				}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{action: actionTypeWriteRecord, key: "k1", identity: "base:k1"},
				{action: actionTypeWriteRecord, key: "k3", identity: "base:k3"},
				{action: actionTypeWriteRecord, key: "k4", identity: "source:k4"},
				{action: actionTypeWriteRecord, key: "k5", identity: "base:k5"},
				{action: actionTypeWriteRecord, key: "k6", identity: "base:k6"},
				{action: actionTypeWriteRecord, key: "k10", identity: "source:k10"},
			},
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{added: 2}},
		},
		"dest range - overlapping ranges": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "dest:k1-k10", MinKey: committed.Key("k1"), MaxKey: committed.Key("k10"), Count: 6, EstimatedSize: 66666}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k4", "dest:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"}, {"k10", "dest:k10"},
				}},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{action: actionTypeWriteRange, rng: committed.Range{ID: "dest:k1-k10", MinKey: committed.Key("k1"), MaxKey: committed.Key("k10"), Count: 6, EstimatedSize: 66666}},
			},
			expectedSummary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
			expectedErr:     graveler.ErrNoChanges,
		},
		"source - remove at end of range": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 4444}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k5", "base:k5"}, {"k6", "base:k6"},
				}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "dest:k1-k10", MinKey: committed.Key("k1"), MaxKey: committed.Key("k10"), Count: 6, EstimatedSize: 66666}, records: []testValueRecord{
					{"k1", "base:k1"}, {"k3", "base:k3"}, {"k4", "dest:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"}, {"k10", "dest:k10"},
				}},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{action: actionTypeWriteRange, rng: committed.Range{ID: "dest:k1-k10", MinKey: committed.Key("k1"), MaxKey: committed.Key("k10"), Count: 6, EstimatedSize: 66666}},
			},
			expectedSummary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
			expectedErr:     graveler.ErrNoChanges,
		},
		"both added key to range": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k6", "base:k6"},
				}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "source:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k2", "source:k2"},
					{"k6", "base:k6"},
				}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "dest:k1-k6", MinKey: committed.Key("k1"), MaxKey: committed.Key("k6")}, records: []testValueRecord{
					{"k1", "base:k1"},
					{"k3", "dest:k3"},
					{"k6", "base:k6"},
				}},
			},
			conflictExpectedIdx: nil,
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
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{added: 1}},
		},
		"source range removed": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			},
			sourceRange: []testRange{{
				rng:     committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
				records: nil,
			}},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{
					action: actionTypeWriteRange,
					rng:    committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
				},
			},
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{removed: 2}},
			expectedErr:     nil,
		},
		"dest range removed": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{
					action: actionTypeWriteRange,
					rng:    committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
				},
			},
			expectedSummary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
			expectedErr:     graveler.ErrNoChanges,
		},
		"source key removed from range - same bounds": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "source:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k6", "base:k6"},
					},
				},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{action: actionTypeWriteRange, rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{action: actionTypeWriteRange, rng: committed.Range{ID: "source:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			},
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{}, Incomplete: true},
		},
		"source key removed from range": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "source:k3-k5", MinKey: committed.Key("k3"), MaxKey: committed.Key("k5"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k5", "base:k5"},
					},
				},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{action: actionTypeWriteRange, rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{action: actionTypeWriteRecord, key: "k3", identity: "base:k3"},
				{action: actionTypeWriteRecord, key: "k5", identity: "base:k5"},
			},
			expectedSummary: graveler.DiffSummary{Count: map[graveler.DiffType]int{removed: 2}},
		},
		"dest key removed from range": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 4, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"}, {"k5", "base:k5"}, {"k6", "base:k6"},
					},
				},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "dest:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k6", "base:k6"},
					},
				},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{action: actionTypeWriteRange, rng: committed.Range{ID: "base:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{action: actionTypeWriteRange, rng: committed.Range{ID: "dest:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			},
			expectedSummary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
			expectedErr:     graveler.ErrNoChanges,
		},
		"empty source and base": {
			baseRange:   []testRange{},
			sourceRange: []testRange{},
			destRange: []testRange{
				{rng: committed.Range{ID: "dest:k1-k2", MinKey: committed.Key("k1"), MaxKey: committed.Key("k2"), Count: 2, EstimatedSize: 1234}},
				{rng: committed.Range{ID: "base:k3-k6", MinKey: committed.Key("k3"), MaxKey: committed.Key("k6"), Count: 2, EstimatedSize: 1234}},
			},
			conflictExpectedIdx: nil,
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
			expectedSummary: graveler.DiffSummary{Count: make(map[graveler.DiffType]int)},
			expectedErr:     graveler.ErrNoChanges,
		},
	}

	for name, tst := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ctx := context.Background()
			writer := mock.NewMockMetaRangeWriter(ctrl)
			for _, action := range tst.expectedActions {
				switch action.action {
				case actionTypeWriteRecord:
					writer.EXPECT().WriteRecord(newRecordMatcher(action.key, action.identity))
				case actionTypeWriteRange:
					writer.EXPECT().WriteRange(gomock.Eq(action.rng))
				}
			}
			metaRangeManager := mock.NewMockMetaRangeManager(ctrl)
			metaRangeManager.EXPECT().NewWriter(gomock.Any(), gomock.Any(), gomock.Any()).Return(writer)
			sourceKey := graveler.MetaRangeID("source")
			destKey := graveler.MetaRangeID("dest")
			baseKey := graveler.MetaRangeID("base")
			metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), sourceKey).Return(createIter(tst.sourceRange), nil)
			metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), destKey).Return(createIter(tst.destRange), nil)
			metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), baseKey).Return(createIter(tst.baseRange), nil)
			metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), destKey).Return(createIter(tst.destRange), nil)

			writer.EXPECT().Abort()
			metaRangeId := graveler.MetaRangeID("merge")
			writer.EXPECT().Close().Return(&metaRangeId, nil).AnyTimes()
			committedManager := committed.NewCommittedManager(metaRangeManager)
			_, summary, err := committedManager.Merge(ctx, "ns", "dest", "source", "base")
			if err != tst.expectedErr {
				t.Fatal(err)
			}
			if diff := deep.Equal(summary, tst.expectedSummary); diff != nil {
				t.Error("LoadActions() found diff", diff)
			}
		})
	}
}
