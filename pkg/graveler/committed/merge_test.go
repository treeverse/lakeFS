package committed_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

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
			expectedErr: nil,
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
			expectedErr: nil,
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
			expectedErr: nil,
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
				{action: actionTypeWriteRecord, key: "k1", identity: "base:k1"},
				{action: actionTypeWriteRecord, key: "k3", identity: "base:k3"},
				{action: actionTypeWriteRecord, key: "k4", identity: "dest:k4"},
				{action: actionTypeWriteRecord, key: "k5", identity: "base:k5"},
				{action: actionTypeWriteRecord, key: "k6", identity: "base:k6"},
				{action: actionTypeWriteRecord, key: "k10", identity: "dest:k10"},
			},
			expectedErr: nil,
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
				{action: actionTypeWriteRecord, key: "k1", identity: "base:k1"},
				{action: actionTypeWriteRecord, key: "k3", identity: "base:k3"},
				{action: actionTypeWriteRecord, key: "k4", identity: "dest:k4"},
				{action: actionTypeWriteRecord, key: "k5", identity: "base:k5"},
				{action: actionTypeWriteRecord, key: "k6", identity: "base:k6"},
				{action: actionTypeWriteRecord, key: "k10", identity: "dest:k10"},
			},
			expectedErr: nil,
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
			expectedErr: nil,
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
			expectedErr: nil,
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
			expectedErr: nil,
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
			expectedErr: nil,
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
			metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), baseKey).Return(createIter(tst.baseRange), nil)
			metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), sourceKey).Return(createIter(tst.sourceRange), nil)
			metaRangeManager.EXPECT().NewMetaRangeIterator(gomock.Any(), gomock.Any(), destKey).Return(createIter(tst.destRange), nil)

			writer.EXPECT().Abort()
			metaRangeId := graveler.MetaRangeID("merge")
			writer.EXPECT().Close().Return(&metaRangeId, nil).AnyTimes()
			committedManager := committed.NewCommittedManager(metaRangeManager)
			_, _, err := committedManager.Merge(ctx, "ns", "dest", "source", "base")
			if err != tst.expectedErr {
				t.Fatal(err)
			}
		})
	}
}

func TestMergeAdd(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	base := testutil.NewFakeIterator()
	base.
		AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 2}).
		AddValueRecords(makeV("a", "base:a"), makeV("c", "base:c")).
		AddRange(&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("d"), Count: 1}).
		AddValueRecords(makeV("d", "base:d"))

	destination := testutil.NewFakeIterator()
	destination.
		AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 2}).
		AddValueRecords(makeV("a", "base:a"), makeV("c", "base:c")).
		AddRange(&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("d"), Count: 1}).
		AddValueRecords(makeV("d", "base:d"))

	source := testutil.NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "b", MinKey: committed.Key("b"), MaxKey: committed.Key("f"), Count: 3}).
		AddValueRecords(makeV("b", "dest:b"), makeV("e", "dest:e"), makeV("f", "dest:f"))
	writer := mock.NewMockMetaRangeWriter(ctrl)

	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "dest:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "dest:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "dest:f")))

	_, err := committed.Merge(context.Background(), writer, base, source, destination)
	assert.NoError(t, err)
}

func TestMergeSameBounds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	base := testutil.NewFakeIterator()
	base.AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "base:a"), makeV("c", "base:c"))

	source := testutil.NewFakeIterator()
	source.AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "base:a"), makeV("c", "base:c"))

	dest := testutil.NewFakeIterator()
	dest.AddRange(&committed.Range{ID: "two", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "base:a"), makeV("b", "dest:b"), makeV("c", "base:c"))

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRange(gomock.Eq(committed.Range{ID: "two", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}))

	_, err := committed.Merge(context.Background(), writer, base, source, dest)
	assert.NoError(t, err)
}

func TestMergeDeleteSource(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	base := testutil.NewFakeIterator()
	base.AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "base:a"), makeV("b", "base:b"), makeV("c", "base:c"))

	source := testutil.NewFakeIterator()
	source.AddRange(&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}).
		AddValueRecords(makeV("d", "base:d"))

	dest := testutil.NewFakeIterator()
	dest.AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "base:a"), makeV("b", "base:b"), makeV("c", "base:c"))

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRange(gomock.Eq(committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}))

	_, err := committed.Merge(context.Background(), writer, base, source, dest)
	assert.NoError(t, err)
}

func TestMergeLeftoverSource(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	base := testutil.NewFakeIterator()
	base.
		AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "base:a"), makeV("b", "base:b"), makeV("c", "base:c")).
		AddRange(&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}).
		AddValueRecords(makeV("d", "base:d"))
	source := testutil.NewFakeIterator()
	source.
		AddRange(&committed.Range{ID: "source:one", MinKey: committed.Key("b"), MaxKey: committed.Key("f"), Count: 3}).
		AddValueRecords(makeV("b", "base:b"), makeV("e", "source:e"), makeV("f", "source:f"))
	dest := testutil.NewFakeIterator()
	dest.
		AddRange(&committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}).
		AddValueRecords(makeV("a", "base:a"), makeV("b", "base:b"), makeV("c", "base:c")).
		AddRange(&committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}).
		AddValueRecords(makeV("d", "base:d"))

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("b", "base:b")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("e", "source:e")))
	writer.EXPECT().WriteRecord(gomock.Eq(*makeV("f", "source:f")))
	summary, err := committed.Merge(context.Background(), writer, base, source, dest)
	assert.NoError(t, err)
	assert.Equal(t, graveler.DiffSummary{
		Count: map[graveler.DiffType]int{}, Incomplete: true,
	}, summary)
}

func TestMergeNoChanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	range1 := &committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}
	range2 := &committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}
	base := testutil.NewFakeIterator().AddRange(range1).
		AddValueRecords(makeV("a", "base:a"), makeV("b", "base:b"), makeV("c", "base:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "base:d"))

	source := testutil.NewFakeIterator().AddRange(range1).
		AddValueRecords(makeV("a", "base:a"), makeV("b", "base:b"), makeV("c", "base:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "base:d"))

	destination := testutil.NewFakeIterator().AddRange(range1).
		AddValueRecords(makeV("a", "base:a"), makeV("b", "base:b"), makeV("c", "base:c")).
		AddRange(range2).
		AddValueRecords(makeV("d", "base:d"))

	writer := mock.NewMockMetaRangeWriter(ctrl)
	writer.EXPECT().WriteRange(gomock.Eq(committed.Range{ID: "one", MinKey: committed.Key("a"), MaxKey: committed.Key("cz"), Count: 3}))
	writer.EXPECT().WriteRange(gomock.Eq(committed.Range{ID: "two", MinKey: committed.Key("d"), MaxKey: committed.Key("dz"), Count: 1}))

	_, err := committed.Merge(context.Background(), writer, base, source, destination)
	assert.NoError(t, err)
}

func TestMergeOpCancelContext(t *testing.T) {
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
		_, err := committed.Merge(ctx, writer, base, source, destination)
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
		_, err := committed.Merge(ctx, writer, base, source, destination)
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
		_, err := committed.Merge(ctx, writer, base, source, destination)
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
		_, err := committed.Merge(ctx, writer, base, source, destination)
		assert.True(t, errors.Is(err, context.Canceled), "context canceled error")
	})
}
