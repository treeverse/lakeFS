package onboard_test

import (
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/onboard"
	"reflect"
	"testing"
)

func TestDiff(t *testing.T) {
	data := []struct {
		LeftInv             []block.InventoryObject
		RightInv            []block.InventoryObject
		ExpectedDiffAdded   []string
		ExpectedDiffDeleted []string
	}{
		{
			LeftInv:             rows("a1", "a2", "a3"),
			RightInv:            rows("a1", "a3", "b4"),
			ExpectedDiffAdded:   []string{"b4"},
			ExpectedDiffDeleted: []string{"a2"},
		},
		{
			LeftInv:             rows("a1", "a2", "a3"),
			RightInv:            rows("a1", "a2", "a3"),
			ExpectedDiffAdded:   nil,
			ExpectedDiffDeleted: nil,
		},
		{
			LeftInv:             rows("a1", "a2", "a3"),
			RightInv:            rows("b1", "b2", "b3", "b4", "b5", "b6"),
			ExpectedDiffAdded:   []string{"b1", "b2", "b3", "b4", "b5", "b6"},
			ExpectedDiffDeleted: []string{"a1", "a2", "a3"},
		},
		{
			LeftInv:             rows("a1", "a3", "a4"),
			RightInv:            rows("a1", "a2", "a3", "a4"),
			ExpectedDiffAdded:   []string{"a2"},
			ExpectedDiffDeleted: nil,
		},
		{
			LeftInv:             rows("a1", "a2", "a3", "a4"),
			RightInv:            rows("a1", "a2", "a4"),
			ExpectedDiffAdded:   nil,
			ExpectedDiffDeleted: []string{"a3"},
		},
		{
			LeftInv:             rows("a1", "a2", "a3", "a4", "a5"),
			RightInv:            rows("b1", "b2"),
			ExpectedDiffAdded:   []string{"b1", "b2"},
			ExpectedDiffDeleted: []string{"a1", "a2", "a3", "a4", "a5"},
		},
		{
			LeftInv:             rows(),
			RightInv:            rows("b1", "b2"),
			ExpectedDiffAdded:   []string{"b1", "b2"},
			ExpectedDiffDeleted: nil,
		},
		{
			LeftInv:             rows("b1", "b2"),
			RightInv:            rows(),
			ExpectedDiffAdded:   nil,
			ExpectedDiffDeleted: []string{"b1", "b2"},
		},
	}
	for _, test := range data {
		diff := onboard.CalcDiff(test.LeftInv, test.RightInv)
		if !reflect.DeepEqual(keys(diff.AddedOrChanged), test.ExpectedDiffAdded) {
			t.Fatalf("diff added object different than expected. expected: %v, got: %v", test.ExpectedDiffAdded, keys(diff.AddedOrChanged))
		}
		if !reflect.DeepEqual(keys(diff.Deleted), test.ExpectedDiffDeleted) {
			t.Fatalf("diff deleted object different than expected. expected: %v, got: %v", test.ExpectedDiffDeleted, keys(diff.Deleted))
		}
	}
}
