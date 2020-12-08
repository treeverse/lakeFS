package tree

import (
	"testing"

	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/forest/sstable"
)

func Test(t *testing.T) {
	tests := []struct {
		left                 *Tree
		right                *Tree
		expectedLeftPartIds  []string
		expectedRightPartIds []string
	}{
		{
			left:                 newTestTree([]string{"A", "B", "C"}, []string{"a9", "b9", "c9"}),
			right:                newTestTree([]string{"A", "C"}, []string{"a9", "c9"}),
			expectedLeftPartIds:  []string{"B"},
			expectedRightPartIds: []string{},
		},
		{
			left:                 newTestTree([]string{"A", "B", "C"}, []string{"a9", "b9", "c9"}),
			right:                newTestTree([]string{"A", "B1", "C"}, []string{"a9", "b9", "c9"}),
			expectedLeftPartIds:  []string{"B"},
			expectedRightPartIds: []string{"B1"},
		},
		{
			left:                 newTestTree([]string{"A", "B", "C"}, []string{"a9", "b99", "c9"}),
			right:                newTestTree([]string{"A", "B1", "C"}, []string{"a9", "b9", "c9"}),
			expectedLeftPartIds:  []string{"B"},
			expectedRightPartIds: []string{"B1"},
		},
		{
			left:                 newTestTree([]string{"A", "B", "C"}, []string{"a9", "b9", "c9"}),
			right:                newTestTree([]string{"A", "B1", "C"}, []string{"a9", "b99", "c9"}),
			expectedLeftPartIds:  []string{"B"},
			expectedRightPartIds: []string{"B1"},
		},
		{
			left:                 newTestTree([]string{"A", "B", "D"}, []string{"a9", "b9", "d9"}),
			right:                newTestTree([]string{"A", "C", "D", "E"}, []string{"a9", "c9", "d9", "e9"}),
			expectedLeftPartIds:  []string{"B"},
			expectedRightPartIds: []string{"C", "E"},
		},
		{
			left:                 newTestTree([]string{"A", "D", "E"}, []string{"a9", "d9", "e9"}),
			right:                newTestTree([]string{"A", "B", "C", "D", "F"}, []string{"a9", "b9", "c9", "d9", "f9"}),
			expectedLeftPartIds:  []string{"E"},
			expectedRightPartIds: []string{"B", "C", "F"},
		},
		{
			left:                 newTestTree([]string{"A", "B", "C", "D", "F"}, []string{"a9", "b9", "c9", "d9", "f9"}),
			right:                newTestTree([]string{"A", "D", "E"}, []string{"a9", "d9", "e9"}),
			expectedLeftPartIds:  []string{"B", "C", "F"},
			expectedRightPartIds: []string{"E"},
		},
		{
			left:                 newTestTree([]string{"Z"}, []string{"z9"}),
			right:                newTestTree([]string{"A", "B", "C", "D", "E"}, []string{"a9", "b9", "c9", "d9", "e9"}),
			expectedLeftPartIds:  []string{"Z"},
			expectedRightPartIds: []string{"A", "B", "C", "D", "E"},
		},
		{
			left:                 newTestTree([]string{"A", "B", "C", "D", "E"}, []string{"a9", "b9", "c9", "d9", "e9"}),
			right:                newTestTree([]string{"Z"}, []string{"z9"}),
			expectedLeftPartIds:  []string{"A", "B", "C", "D", "E"},
			expectedRightPartIds: []string{"Z"},
		},
	}
	for _, tst := range tests {
		gotLeft, gotRight := removeDuplicates(tst.left, tst.right)
		if len(gotLeft.parts) != len(tst.expectedLeftPartIds) {
			t.Fatalf("got unexpected number of parts on left tree. expected=%d, got=%d", len(tst.expectedLeftPartIds), len(gotLeft.parts))
		}
		for i := range gotLeft.parts {
			if string(gotLeft.parts[i].Name) != tst.expectedLeftPartIds[i] {
				t.Fatalf("unexpected part id in new left tree index %d: expected=%s, got=%s", i, tst.expectedLeftPartIds[i], string(gotLeft.parts[i].Name))
			}
		}
		if len(gotRight.parts) != len(tst.expectedRightPartIds) {
			t.Fatalf("got unexpected number of parts on right tree. expected=%d, got=%d", len(tst.expectedRightPartIds), len(gotRight.parts))
		}
		for i := range gotRight.parts {
			if string(gotRight.parts[i].Name) != tst.expectedRightPartIds[i] {
				t.Fatalf("unexpected part id in new right tree index %d: expected=%s, got=%s", i, tst.expectedRightPartIds[i], string(gotRight.parts[i].Name))
			}
		}
	}
}

func newTestTree(partIds []string, maxPaths []string) *Tree {
	parts := make([]*Part, 0, len(partIds))
	for i := range partIds {
		parts = append(parts, &Part{
			Name:    sstable.ID(partIds[i]),
			MaxPath: rocks.Path(maxPaths[i]),
		})
	}
	return &Tree{
		parts: parts,
	}
}
