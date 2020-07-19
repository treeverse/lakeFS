package onboard_test

import (
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/onboard"
	"reflect"
	"testing"
)

func TestDiff(t *testing.T) {
	data := []struct {
		LeftInv             <-chan *block.InventoryObject
		RightInv            <-chan *block.InventoryObject
		ExpectedDiffAdded   []string
		ExpectedDiffDeleted []string
	}{
		{
			LeftInv:             objects("a1", "a2", "a3"),
			RightInv:            objects("a1", "a3", "b4"),
			ExpectedDiffAdded:   []string{"b4"},
			ExpectedDiffDeleted: []string{"a2"},
		},
		{
			LeftInv:             objects("a1", "a2", "a3"),
			RightInv:            objects("a1", "a2", "a3"),
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{},
		},
		{
			LeftInv:             objects("a1", "a2", "a3"),
			RightInv:            objects("b1", "b2", "b3", "b4", "b5", "b6"),
			ExpectedDiffAdded:   []string{"b1", "b2", "b3", "b4", "b5", "b6"},
			ExpectedDiffDeleted: []string{"a1", "a2", "a3"},
		},
		{
			LeftInv:             objects("a1", "a3", "a4"),
			RightInv:            objects("a1", "a2", "a3", "a4"),
			ExpectedDiffAdded:   []string{"a2"},
			ExpectedDiffDeleted: []string{},
		},
		{
			LeftInv:             objects("a1", "a2", "a3", "a4"),
			RightInv:            objects("a1", "a2", "a4"),
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{"a3"},
		},
		{
			LeftInv:             objects("a1", "a2", "a3", "a4", "a5"),
			RightInv:            objects("b1", "b2"),
			ExpectedDiffAdded:   []string{"b1", "b2"},
			ExpectedDiffDeleted: []string{"a1", "a2", "a3", "a4", "a5"},
		},
		{
			LeftInv:             objects(),
			RightInv:            objects("b1", "b2"),
			ExpectedDiffAdded:   []string{"b1", "b2"},
			ExpectedDiffDeleted: []string{},
		},
		{
			LeftInv:             objects("b1", "b2"),
			RightInv:            objects(),
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{"b1", "b2"},
		},
	}
	for _, test := range data {
		in := onboard.CalcDiff(test.LeftInv, test.RightInv)
		actualAdded := make([]string, 0, len(test.ExpectedDiffAdded))
		actualDeleted := make([]string, 0, len(test.ExpectedDiffDeleted))
		for o := range in {
			if o.ToDelete {
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
