package committed_test

import (
	"testing"

	"github.com/treeverse/lakefs/graveler/committed"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/mock"
	"github.com/treeverse/lakefs/graveler/testutil"
)

var (
	one   = committed.ID("range1")
	two   = committed.ID("range2")
	three = committed.ID("range3")
	four  = committed.ID("range4")
	five  = committed.ID("range5")
)

func makeRange(rangeIDs []graveler.Key) committed.ValueIterator {
	records := make([]committed.Record, len(rangeIDs))
	for i, id := range rangeIDs {
		records[i] = committed.Record{Key: committed.Key(id)}
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

func keysByRanges(t testing.TB, it committed.Iterator) []rangeKeys {
	t.Helper()
	ret := make([]rangeKeys, 0)
	for it.Next() {
		v, p := it.Value()
		require.True(t, p != nil, "iterated past end, it = %+v", it)
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
	tests := []struct {
		Name string
		PK   []rangeKeys
	}{
		{
			Name: "empty",
			PK:   []rangeKeys{},
		}, {
			Name: "one empty",
			PK:   []rangeKeys{{Name: one, Keys: makeKeys()}},
		}, {
			Name: "many empty",
			PK: []rangeKeys{
				{Name: one, Keys: makeKeys()},
				{Name: two, Keys: makeKeys()},
				{Name: three, Keys: makeKeys()},
			},
		}, {
			Name: "one",
			PK:   []rangeKeys{{Name: one, Keys: makeKeys("a1", "a2", "a3")}},
		}, {
			Name: "five ranges two empty",
			PK: []rangeKeys{
				{Name: one, Keys: makeKeys("a1", "a2", "a3")},
				{Name: two, Keys: makeKeys()},
				{Name: three, Keys: makeKeys()},
				{Name: four, Keys: makeKeys("d1")},
				{Name: five, Keys: makeKeys("e1", "e2")},
			},
		}, {
			Name: "last two empty",
			PK: []rangeKeys{
				{Name: one, Keys: makeKeys("a1", "a2", "a3")},
				{Name: two, Keys: makeKeys()},
				{Name: three, Keys: makeKeys()},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			manager := mock.NewMockRangeManager(ctrl)

			namespace := committed.Namespace("ns")
			ranges := make([]committed.Range, 0, len(tt.PK))
			for _, p := range tt.PK {
				// MaxKey unused
				ranges = append(ranges, committed.Range{ID: p.Name})
				manager.EXPECT().NewRangeIterator(gomock.Eq(namespace), p.Name, nil).Return(makeRange(p.Keys), nil)
			}
			pvi := committed.NewIterator(manager, namespace, ranges)
			assert.Equal(t, tt.PK, keysByRanges(t, pvi))
			assert.False(t, pvi.NextRange())
			assert.False(t, pvi.Next())
		})
	}
}
