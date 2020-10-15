package onboard_test

import (
	"testing"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/onboard"
)

func generateLastModified(keys []string, times map[string]time.Time) []time.Time {
	res := make([]time.Time, len(keys))
	for i, k := range keys {
		res[i] = times[k]
	}
	return res
}
func TestDiff(t *testing.T) {
	data := []struct {
		LeftInv             []string
		RightInv            []string
		ExpectedDiffAdded   []string
		ExpectedDiffDeleted []string
		ChangedFiles        []string
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
		{
			LeftInv:             []string{"a1", "a2", "a3"},
			RightInv:            []string{"a1", "a3", "b4"},
			ExpectedDiffAdded:   []string{"b4"},
			ExpectedDiffDeleted: []string{"a2"},
			ChangedFiles:        []string{"a3"},
		},
		{
			LeftInv:             []string{"a1", "a2", "a3"},
			RightInv:            []string{"a1", "a2", "a3"},
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{},
			ChangedFiles:        []string{"a2"},
		},
		{
			LeftInv:             []string{"a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3"},
			RightInv:            []string{"b3"},
			ExpectedDiffAdded:   []string{},
			ExpectedDiffDeleted: []string{"a1", "a2", "a3", "a4", "a5", "b1", "b2"},
			ChangedFiles:        []string{"b3"},
		},
		{
			LeftInv:             []string{"a1"},
			RightInv:            []string{"a1", "a2", "a3", "a4", "a5", "b1", "b2", "b3"},
			ExpectedDiffAdded:   []string{"a2", "a3", "a4", "a5", "b1", "b2", "b3"},
			ExpectedDiffDeleted: []string{},
			ChangedFiles:        []string{"a1"},
		},
	}
	for _, test := range data {
		now := time.Now()
		times := map[string]time.Time{
			"a1": now,
			"a2": now.Add(-1 * time.Hour),
			"a3": now.Add(-2 * time.Hour),
			"a4": now.Add(-3 * time.Hour),
			"a5": now.Add(-4 * time.Hour),
			"b1": now,
			"b2": now.Add(-1 * time.Hour),
			"b3": now.Add(-2 * time.Hour),
			"b4": now.Add(-3 * time.Hour),
			"b5": now.Add(-4 * time.Hour),
			"b6": now.Add(-5 * time.Hour),
		}
		rightInvChecksum := func(s string) string {
			for _, changed := range test.ChangedFiles {
				if changed == s {
					return "abcde" + s
				}
			}
			return s
		}
		rightInv := &mockInventory{keys: test.RightInv, lastModified: generateLastModified(test.RightInv, times), checksum: rightInvChecksum}
		leftInv := &mockInventory{keys: test.LeftInv, lastModified: generateLastModified(test.LeftInv, times)}
		leftIt := leftInv.Iterator()
		rightIt := rightInv.Iterator()
		it := onboard.NewDiffIterator(leftIt, rightIt)
		actualAdded := make([]*block.InventoryObject, 0, len(test.ExpectedDiffAdded))
		actualDeleted := make([]*block.InventoryObject, 0, len(test.ExpectedDiffDeleted))
		actualChanged := make([]*block.InventoryObject, 0, len(test.ChangedFiles))
		for it.Next() {
			o := it.Get()
			if o.IsDeleted {
				actualDeleted = append(actualDeleted, &o.Obj)
			} else if o.IsChanged {
				actualChanged = append(actualChanged, &o.Obj)
			} else {
				actualAdded = append(actualAdded, &o.Obj)
			}
		}
		if len(actualAdded) != len(test.ExpectedDiffAdded) {
			t.Fatalf("number of added objects in diff different than expected. expected: %d, got: %d", len(test.ExpectedDiffAdded), len(actualAdded))
		}
		if len(actualDeleted) != len(test.ExpectedDiffDeleted) {
			t.Fatalf("number of deleted objects in diff different than expected. expected: %d, got: %d", len(test.ExpectedDiffDeleted), len(actualDeleted))
		}
		if len(actualChanged) != len(test.ChangedFiles) {
			t.Fatalf("number of changed objects in diff different than expected. expected: %d, got: %d", len(test.ChangedFiles), len(actualChanged))
		}
		for i, expectedAdded := range test.ExpectedDiffAdded {
			if actualAdded[i].Key != expectedAdded {
				t.Fatalf("added object in diff index %d different than expected. expected: %s, got: %s", i, expectedAdded, actualAdded[i].Key)
			}
			if *actualAdded[i].LastModified != times[expectedAdded] {
				t.Fatalf("modified time for key %s different than expected. expected: %v, got: %v", expectedAdded, times[expectedAdded], actualAdded[i].LastModified)
			}
		}
		for i, expectedDeleted := range test.ExpectedDiffDeleted {
			if actualDeleted[i].Key != expectedDeleted {
				t.Fatalf("deleted object in diff index %d different than expected. expected: %s, got: %s", i, expectedDeleted, actualDeleted[i].Key)
			}
			if *actualDeleted[i].LastModified != times[expectedDeleted] {
				t.Fatalf("modified time for key %s different than expected. expected: %v, got: %v", expectedDeleted, times[expectedDeleted], actualDeleted[i].LastModified)
			}
		}
		for i, expectedChanged := range test.ChangedFiles {
			if actualChanged[i].Key != expectedChanged {
				t.Fatalf("changed object in diff index %d different than expected. expected: %s, got: %s", i, expectedChanged, actualChanged[i].Key)
			}
			if *actualChanged[i].LastModified != times[expectedChanged] {
				t.Fatalf("modified time for key %s different than expected. expected: %v, got: %v", expectedChanged, times[expectedChanged], actualChanged[i].LastModified)
			}
		}
	}
}
