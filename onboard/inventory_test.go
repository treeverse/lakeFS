package onboard_test

import (
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/onboard"
)

func TestDiff(t *testing.T) {
	data := []struct {
		LeftInv             []string
		RightInv            []string
		ExpectedDiffAdded   []string
		ExpectedDiffDeleted []string
	}{
		{
			LeftInv:             []string{"a1", "a2", "a3"},
			RightInv:            []string{"a1", "a3", "b4"},
			ExpectedDiffAdded:   []string{"b4"},
			ExpectedDiffDeleted: []string{"a2"},
		},
		{
			LeftInv:             []string{"a1", "a2", "a3"},
			RightInv:            []string{"a1", "a2", "a3"},
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{},
		},
		{
			LeftInv:             []string{"a1", "a2", "a3"},
			RightInv:            []string{"b1", "b2", "b3", "b4", "b5", "b6"},
			ExpectedDiffAdded:   []string{"b1", "b2", "b3", "b4", "b5", "b6"},
			ExpectedDiffDeleted: []string{"a1", "a2", "a3"},
		},
		{
			LeftInv:             []string{"a1", "a3", "a4"},
			RightInv:            []string{"a1", "a2", "a3", "a4"},
			ExpectedDiffAdded:   []string{"a2"},
			ExpectedDiffDeleted: []string{},
		},
		{
			LeftInv:             []string{"a1", "a2", "a3", "a4"},
			RightInv:            []string{"a1", "a2", "a4"},
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{"a3"},
		},
		{
			LeftInv:             []string{"a1", "a2", "a3", "a4", "a5"},
			RightInv:            []string{"b1", "b2"},
			ExpectedDiffAdded:   []string{"b1", "b2"},
			ExpectedDiffDeleted: []string{"a1", "a2", "a3", "a4", "a5"},
		},
		{
			LeftInv:             []string{},
			RightInv:            []string{"b1", "b2"},
			ExpectedDiffAdded:   []string{"b1", "b2"},
			ExpectedDiffDeleted: []string{},
		},
		{
			LeftInv:             []string{"b1", "b2"},
			RightInv:            []string{},
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{"b1", "b2"},
		},
	}
	for _, test := range data {
		rightInv := &mockInventory{rows: test.RightInv}
		leftInv := &mockInventory{rows: test.LeftInv}
		leftIt := leftInv.Iterator()
		rightIt := rightInv.Iterator()
		it := onboard.NewDiffIterator(leftIt, rightIt)
		actualAdded := make([]string, 0, len(test.ExpectedDiffAdded))
		actualDeleted := make([]string, 0, len(test.ExpectedDiffDeleted))
		for it.Next() {
			o := it.Get()
			if o.IsDeleted {
				actualDeleted = append(actualDeleted, o.Obj.Key)
			} else {
				actualAdded = append(actualAdded, o.Obj.Key)
			}
		}
		if !reflect.DeepEqual(actualAdded, test.ExpectedDiffAdded) {
			t.Fatalf("diff added object different than expected. expected: %v, got: %v", test.ExpectedDiffAdded, actualAdded)
		}
		if !reflect.DeepEqual(actualDeleted, test.ExpectedDiffDeleted) {
			t.Fatalf("diff deleted object different than expected. expected: %v, got: %v", test.ExpectedDiffDeleted, actualDeleted)
		}
	}
}
