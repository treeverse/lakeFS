package committed_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
		"dest removed range and added range after source removed range edges": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k5", MinKey: committed.Key("k1"), MaxKey: committed.Key("k5"), Count: 5, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k1", "base:k1"}, {"k2", "base:k2"}, {"k3", "base:k3"}, {"k4", "base:k4"},
						{"k5", "base:k5"},
					},
				},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "source:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"},
					},
				},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "dest:k6-k7", MinKey: committed.Key("k6"), MaxKey: committed.Key("k7"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k6", "dest:k6"}, {"k7", "dest:k7"},
					},
				},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{action: actionTypeWriteRecord, key: "k6", identity: "dest:k6"},
				{action: actionTypeWriteRecord, key: "k7", identity: "dest:k7"},
			},
			expectedErr: nil,
		},
		"source and dest change same range conflict": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k1-k5", MinKey: committed.Key("k1"), MaxKey: committed.Key("k5"), Count: 5, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k1", "base:k1"}, {"k2", "base:k2"}, {"k3", "base:k3"}, {"k4", "base:k4"},
						{"k5", "base:k5"},
					},
				},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "source:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "source:k4"},
					},
				},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "dest:k6-k7", MinKey: committed.Key("k6"), MaxKey: committed.Key("k7"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k6", "dest:k6"}, {"k7", "dest:k7"},
					},
				},
			},
			conflictExpectedIdx: nil,
			expectedActions:     []writeAction{},
			expectedErr:         graveler.ErrConflictFound,
		},
		"no changes": {
			baseRange: []testRange{
				{rng: committed.Range{ID: "base:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"},
					},
				},
			},
			sourceRange: []testRange{
				{rng: committed.Range{ID: "base:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"},
					},
				},
			},
			destRange: []testRange{
				{rng: committed.Range{ID: "base:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
					records: []testValueRecord{
						{"k3", "base:k3"}, {"k4", "base:k4"},
					},
				},
			},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{
					action: actionTypeWriteRange,
					rng:    committed.Range{ID: "base:k3-k4", MinKey: committed.Key("k3"), MaxKey: committed.Key("k4"), Count: 2, EstimatedSize: 1234},
				},
			},
			expectedErr: nil,
		},
		"source and dest changed record identity": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:b-c", MinKey: committed.Key("b"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}, {key: "c", identity: "c"}},
				},
				{
					rng:     committed.Range{ID: "base:d-e", MinKey: committed.Key("d"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "d", identity: "d"}, {key: "e", identity: "e"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "d", identity: "d"}},
				},
				{
					rng:     committed.Range{ID: "source:e", MinKey: committed.Key("e"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "e", identity: "e1"}},
				},
			},
			destRange: []testRange{
				{
					rng:     committed.Range{ID: "base:b-c", MinKey: committed.Key("b"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}, {key: "c", identity: "c"}},
				},
				{
					rng:     committed.Range{ID: "dest:d-e", MinKey: committed.Key("d"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "d", identity: "d1"}, {key: "e", identity: "e"}},
				},
			},
			conflictExpectedIdx: nil,
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
		},
		"dest removed all source added": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 4, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			},
			destRange:           []testRange{},
			conflictExpectedIdx: nil,
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
		},
		"same identity different key": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 4, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "a1", identity: "a"}, {key: "c", identity: "c"}},
				},
			},
			destRange: []testRange{
				{
					rng:     committed.Range{ID: "dest:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			},
			conflictExpectedIdx: nil,
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
		},
		"dest removed all source range before base": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:c-d", MinKey: committed.Key("c"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
			},
			destRange:           []testRange{},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{
					action: actionTypeWriteRange,
					rng:    committed.Range{ID: "source:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
				},
			},
			expectedErr: nil,
		},
		"dest removed all same key different identity": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a1"}, {key: "b", identity: "b"}},
				},
			},
			destRange:           []testRange{},
			conflictExpectedIdx: nil,
			expectedActions:     []writeAction{},
			expectedErr:         graveler.ErrConflictFound,
		},
		"dest removed all different key different identity": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "a2", identity: "a2"}, {key: "b", identity: "b"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "a1", identity: "a1"}, {key: "b", identity: "b"}},
				},
			},
			destRange:           []testRange{},
			conflictExpectedIdx: nil,
			expectedActions: []writeAction{
				{
					action:   actionTypeWriteRecord,
					key:      "a1",
					identity: "a1",
				},
			},
			expectedErr: nil,
		},
		"dest removed all base and source same identity": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
				{
					rng:     committed.Range{ID: "base:d-f", MinKey: committed.Key("d"), MaxKey: committed.Key("f"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "d", identity: "d"}, {key: "f", identity: "f"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
				{
					rng:     committed.Range{ID: "source:c-e", MinKey: committed.Key("c"), MaxKey: committed.Key("e"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}, {key: "e", identity: "e"}},
				},
			},
			destRange:           []testRange{},
			conflictExpectedIdx: nil,
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
		},
		"dest range before source": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:c-d", MinKey: committed.Key("c"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			},
			destRange: []testRange{
				{
					rng:     committed.Range{ID: "dest:a-b", MinKey: committed.Key("a"), MaxKey: committed.Key("b"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a1"}, {key: "b", identity: "b"}},
				},
			},
			conflictExpectedIdx: nil,
			expectedActions:     []writeAction{},
			expectedErr:         graveler.ErrConflictFound,
		},
		"source key before dest range": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			},
			destRange: []testRange{
				{
					rng:     committed.Range{ID: "dest:b-c", MinKey: committed.Key("b"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}, {key: "c", identity: "c"}},
				},
				{
					rng:     committed.Range{ID: "dest:e-f", MinKey: committed.Key("e"), MaxKey: committed.Key("f"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "e", identity: "e"}, {key: "f", identity: "f"}},
				},
			},
			conflictExpectedIdx: nil,
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
		},
		"dest key before source range": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:b-c", MinKey: committed.Key("b"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "b", identity: "b"}, {key: "c", identity: "c"}},
				},
				{
					rng:     committed.Range{ID: "source:e-f", MinKey: committed.Key("e"), MaxKey: committed.Key("f"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "e", identity: "e"}, {key: "f", identity: "f"}},
				},
			},
			destRange: []testRange{
				{
					rng:     committed.Range{ID: "dest:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "b", identity: "b"}, {key: "c", identity: "c"}, {key: "d", identity: "d"}},
				},
			},
			conflictExpectedIdx: nil,
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
		},
		"dest range before source key": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:c-c", MinKey: committed.Key("c"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}},
				},
			},
			sourceRange: []testRange{
				{
					rng:     committed.Range{ID: "source:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			},
			destRange: []testRange{
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
			},
			conflictExpectedIdx: nil,
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
		},
		"source range before dest key": {
			baseRange: []testRange{
				{
					rng:     committed.Range{ID: "base:c-c", MinKey: committed.Key("c"), MaxKey: committed.Key("c"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "c", identity: "c"}},
				},
			},
			sourceRange: []testRange{
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
			},
			destRange: []testRange{
				{
					rng:     committed.Range{ID: "dest:a-d", MinKey: committed.Key("a"), MaxKey: committed.Key("d"), Count: 2, EstimatedSize: 1024},
					records: []testValueRecord{{key: "a", identity: "a"}, {key: "d", identity: "d"}},
				},
			},
			conflictExpectedIdx: nil,
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
			_, err := committedManager.Merge(ctx, "ns", "dest", "source", "base")
			if err != tst.expectedErr {
				t.Fatal(err)
			}
		})
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
		err := committed.Merge(ctx, writer, base, source, destination)
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
		err := committed.Merge(ctx, writer, base, source, destination)
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
		err := committed.Merge(ctx, writer, base, source, destination)
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
		err := committed.Merge(ctx, writer, base, source, destination)
		assert.True(t, errors.Is(err, context.Canceled), "context canceled error")
	})
}
