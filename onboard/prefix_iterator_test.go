package onboard_test

import (
	"testing"

	"github.com/treeverse/lakefs/onboard"

	"github.com/go-test/deep"
)

func TestPrefixIterator(t *testing.T) {
	data := []struct {
		Inv             []string
		Prefixes        []string
		ExpectedResults []string
	}{
		{
			Inv:             []string{"a1", "a2", "a3", "a4", "b1", "b2", "b3"},
			Prefixes:        []string{"a"},
			ExpectedResults: []string{"a1", "a2", "a3", "a4"},
		},
		{
			Inv:             []string{"a1", "a2", "b1", "b2", "c1", "c2"},
			Prefixes:        []string{"a", "c"},
			ExpectedResults: []string{"a1", "a2", "c1", "c2"},
		},
		{
			Inv:             []string{"a1", "a2", "b1", "b2", "c1", "c2"},
			Prefixes:        []string{"c", "a"},
			ExpectedResults: []string{"a1", "a2", "c1", "c2"},
		},
		{
			Inv:             []string{"a1", "a2", "b1", "b2", "c1", "c2"},
			Prefixes:        []string{"a", "c", "d"},
			ExpectedResults: []string{"a1", "a2", "c1", "c2"},
		},
		{
			Inv:             []string{"a1", "a2", "b1", "b2", "c1", "c2"},
			Prefixes:        []string{"b", "a"},
			ExpectedResults: []string{"a1", "a2", "b1", "b2"},
		},
		{
			Inv:             []string{"a1", "a2", "b1", "b2", "c1", "c2"},
			Prefixes:        []string{"d"},
			ExpectedResults: []string{},
		},
		{
			Inv:             []string{"b1", "b2", "b3"},
			Prefixes:        []string{"a"},
			ExpectedResults: []string{},
		},
		{
			Inv:             []string{"b1", "b2", "b3"},
			Prefixes:        []string{"a", "d"},
			ExpectedResults: []string{},
		},
		{
			Inv:             []string{"b1", "b2", "b3", "c1", "c2"},
			Prefixes:        []string{"a", "b", "d"},
			ExpectedResults: []string{"b1", "b2", "b3"},
		},
	}
	for _, test := range data {
		inv := &mockInventory{keys: test.Inv}
		it := onboard.NewInventoryIterator(inv.Iterator())
		prefixIt := onboard.NewPrefixIterator(it, test.Prefixes)
		res := make([]string, 0, len(test.ExpectedResults))
		for prefixIt.Next() {
			res = append(res, prefixIt.Get().Obj.Key)
		}
		if diff := deep.Equal(test.ExpectedResults, res); diff != nil {
			t.Fatalf("unexpected results after filter. diff=%s", diff)
		}
	}
}
