package committed_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
	"github.com/treeverse/lakefs/pkg/graveler/committed/mock"
	"github.com/treeverse/lakefs/pkg/graveler/testutil"
)

func mustMarshalRange(rng committed.Range) []byte {
	ret, err := committed.MarshalRange(rng)
	if err != nil {
		panic(err)
	}
	return ret
}

func makeValueBytesForRangeKey(key graveler.Key, idx int) []byte {
	return committed.MustMarshalValue(&graveler.Value{
		Identity: []byte(fmt.Sprintf("%s:%d", key, idx)),
		Data:     mustMarshalRange(committed.Range{ID: ""}),
	})
}

func makeRangeIterator(rangeIDs []graveler.Key) committed.ValueIterator {
	records := make([]committed.Record, len(rangeIDs))
	for i, id := range rangeIDs {
		records[i] = committed.Record{
			Key:   committed.Key(id),
			Value: makeValueBytesForRangeKey(id, i),
		}
	}
	return testutil.NewCommittedValueIteratorFake(records)
}

func makeKeys(keys ...string) []graveler.Key {
	if len(keys) == 0 {
		// testify doesn't think nil slices are equal to empty slices
		return nil
	}
	ret := make([]graveler.Key, 0, len(keys))
	for _, k := range keys {
		ret = append(ret, graveler.Key(k))
	}
	return ret
}

type rangeKeys struct {
	Name committed.ID
	Keys []graveler.Key
}

func makeRange(r rangeKeys) committed.Range {
	var ret committed.Range
	if len(r.Keys) > 0 {
		ret.MinKey = committed.Key(r.Keys[0])
		ret.MaxKey = committed.Key(r.Keys[len(r.Keys)-1])
	}
	ret.ID = committed.ID(ret.MaxKey)
	return ret
}

func makeRangeRecords(rk []rangeKeys) []committed.Record {
	ret := make([]committed.Record, len(rk))
	var lastKey committed.Key
	for i := range rk {
		rng := makeRange(rk[i])
		rangeVal, err := committed.MarshalRange(rng)
		if err != nil {
			panic(err)
		}
		if rangeVal == nil {
			panic("nil range")
		}
		key := rng.MaxKey
		if len(key) == 0 {
			// Empty range, MaxKey unchanged
			key = lastKey
		}
		v := graveler.Value{Identity: key, Data: rangeVal}
		vBytes := committed.MustMarshalValue(&v)
		ret[i] = committed.Record{
			Key:   key,
			Value: vBytes,
		}
		lastKey = key
	}
	return ret
}

func keysByRanges(t testing.TB, it committed.Iterator) []rangeKeys {
	t.Helper()
	ret := make([]rangeKeys, 0)
	for it.Next() {
		v, p := it.Value()
		require.True(t, p != nil, "iterated past end, it = %+v, %s", it, it.Err())
		if v == nil {
			t.Logf("new range %+v", p)
			ret = append(ret, rangeKeys{Name: p.ID})
		} else {
			t.Logf("    element %+v", v)
			p := &ret[len(ret)-1]
			p.Keys = append(p.Keys, v.Key)
		}
	}
	if err := it.Err(); err != nil {
		t.Error(err)
	}
	return ret
}

func TestIterator(t *testing.T) {
	namespace := committed.Namespace("ns")
	tests := []struct {
		Name string
		PK   []rangeKeys
	}{
		{
			Name: "empty",
			PK:   []rangeKeys{},
		}, {
			Name: "one empty",
			PK:   []rangeKeys{{Name: "", Keys: makeKeys()}},
		}, {
			Name: "many empty",
			PK: []rangeKeys{
				{Name: "", Keys: makeKeys()},
				{Name: "", Keys: makeKeys()},
				{Name: "", Keys: makeKeys()},
			},
		}, {
			Name: "one",
			PK:   []rangeKeys{{Name: "a3", Keys: makeKeys("a1", "a2", "a3")}},
		}, {
			Name: "five ranges two empty",
			PK: []rangeKeys{
				{Name: "a3", Keys: makeKeys("a1", "a2", "a3")},
				{Name: "a3", Keys: makeKeys()},
				{Name: "a3", Keys: makeKeys()},
				{Name: "d1", Keys: makeKeys("d1")},
				{Name: "e2", Keys: makeKeys("e1", "e2")},
			},
		}, {
			Name: "last two empty",
			PK: []rangeKeys{
				{Name: "a3", Keys: makeKeys("a1", "a2", "a3")},
				{Name: "a3", Keys: makeKeys()},
				{Name: "a3", Keys: makeKeys()},
			},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("Iteration<%s>", tt.Name), func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			manager := mock.NewMockRangeManager(ctrl)

			var lastKey committed.Key
			for _, p := range tt.PK {
				key := lastKey
				if len(p.Keys) > 0 {
					key = committed.Key(p.Keys[len(p.Keys)-1])
				}
				manager.EXPECT().
					NewRangeIterator(gomock.Any(), gomock.Eq(namespace), committed.ID(key)).
					Return(makeRangeIterator(p.Keys), nil)
				lastKey = key
			}
			rangesIt := testutil.NewCommittedValueIteratorFake(makeRangeRecords(tt.PK))
			pvi := committed.NewIterator(ctx, manager, namespace, rangesIt)
			defer pvi.Close()
			assert.Equal(t, tt.PK, keysByRanges(t, pvi))
			assert.False(t, pvi.NextRange())
			assert.False(t, pvi.Next())
		})
		t.Run(fmt.Sprintf("SeekGE<%s>", tt.Name), func(t *testing.T) {
			ctx := context.Background()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			manager := mock.NewMockRangeManager(ctrl)

			var lastKey committed.Key
			for _, p := range tt.PK {
				key := lastKey
				if len(p.Keys) > 0 {
					key = committed.Key(p.Keys[len(p.Keys)-1])
				}
				manager.EXPECT().
					NewRangeIterator(gomock.Any(), gomock.Eq(namespace), committed.ID(key)).
					Return(makeRangeIterator(p.Keys), nil).
					AnyTimes()
				lastKey = key
			}
			rangesIt := testutil.NewCommittedValueIteratorFake(makeRangeRecords(tt.PK))
			pvi := committed.NewIterator(ctx, manager, namespace, rangesIt)
			defer pvi.Close()

			if len(tt.PK) == 0 {
				pvi.SeekGE(graveler.Key("infinity"))
				if pvi.Next() {
					v, r := pvi.Value()
					t.Errorf("successful seeked on a nil range, value %+v,%+v", v, r)
				}
				return
			}

			for _, p := range tt.PK {
				if len(p.Keys) == 0 {
					continue
				}
				for _, k := range p.Keys {
					t.Run(fmt.Sprintf("Exact:%s", string(k)), func(t *testing.T) {
						pvi.SeekGE(k)
						if pvi.Err() != nil {
							t.Fatalf("failed to seek to %s: %s", string(k), pvi.Err())
						}
						if !pvi.Next() {
							t.Fatalf("failed to get value at %s: %s", string(k), pvi.Err())
						}
						v, r := pvi.Value()
						if v == nil && r != nil {
							// got range - validate range
							if string(k) != string(r.MinKey) {
								t.Errorf("got range with MinKey %s != expected %s", string(v.Key), string(k))
							}
							// call next to enter range and receive value
							if !pvi.Next() {
								t.Fatalf("failed to get value at %s: %s", string(k), pvi.Err())
							}
							v, r = pvi.Value()
						}
						if v == nil || r == nil {
							t.Fatalf("missing value-and-range %+v, %+v", v, r)
						}
						if string(k) != string(v.Key) {
							t.Errorf("got value with ID %s != expected %s", string(v.Key), string(k))
						}
						if string(p.Name) != string(r.ID) {
							t.Errorf("got range with ID %s != expected %s", string(r.ID), string(p.Name))
						}
					})
				}
			}
		})
	}
}
