package tree_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/committed/tree"
	"github.com/treeverse/lakefs/graveler/committed/tree/mock"
	"github.com/treeverse/lakefs/graveler/testutil"
)

var (
	one   = committed.ID("part1")
	two   = committed.ID("part2")
	three = committed.ID("part3")
	four  = committed.ID("part4")
	five  = committed.ID("part5")
)

func makePart(partIDs []graveler.Key) graveler.ValueIterator {
	records := make([]graveler.ValueRecord, len(partIDs))
	for i, id := range partIDs {
		records[i] = graveler.ValueRecord{Key: id}
	}
	return testutil.NewValueIteratorFake(records)
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

type partKeys struct {
	Name committed.ID
	Keys []graveler.Key
}

func keysByParts(t testing.TB, it tree.Iterator) []partKeys {
	ret := make([]partKeys, 0)
	for it.Err() == nil && it.Next() {
		v, p := it.Value()
		require.True(t, p != nil, "iterated past end, it = %+v", it)
		if v == nil {
			t.Logf("new part %+v", p)
			ret = append(ret, partKeys{Name: p.ID})
		} else {
			t.Logf("    element %+v", v)
			p := &ret[len(ret)-1]
			p.Keys = append(p.Keys, v.Key)
		}
	}
	if it.Err() != nil {
		t.Error(it.Err())
	}
	return ret
}

func TestIterator(t *testing.T) {
	tests := []struct {
		Name string
		PK   []partKeys
	}{
		{
			Name: "empty",
			PK:   []partKeys{},
		}, {
			Name: "one empty",
			PK:   []partKeys{{Name: one, Keys: makeKeys()}},
		}, {
			Name: "many empty",
			PK: []partKeys{
				{Name: one, Keys: makeKeys()},
				{Name: two, Keys: makeKeys()},
				{Name: three, Keys: makeKeys()},
			},
		}, {
			Name: "one",
			PK:   []partKeys{{Name: one, Keys: makeKeys("a1", "a2", "a3")}},
		}, {
			Name: "five parts two empty",
			PK: []partKeys{
				{Name: one, Keys: makeKeys("a1", "a2", "a3")},
				{Name: two, Keys: makeKeys()},
				{Name: three, Keys: makeKeys()},
				{Name: four, Keys: makeKeys("d1")},
				{Name: five, Keys: makeKeys("e1", "e2")},
			},
		}, {
			Name: "last two empty",
			PK: []partKeys{
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
			repo := mock.NewMockRepo(ctrl)

			parts := make([]tree.Part, 0, len(tt.PK))
			for _, p := range tt.PK {
				// MaxKey unused
				parts = append(parts, tree.Part{ID: p.Name})
				repo.EXPECT().NewPartIterator(p.Name, nil).Return(makePart(p.Keys), nil)
			}
			pvi := tree.NewIterator(repo, parts)
			assert.Equal(t, tt.PK, keysByParts(t, pvi))
			assert.False(t, pvi.NextPart())
			assert.False(t, pvi.Next())
		})
	}
}
