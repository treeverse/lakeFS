package tree_test

import (
	"testing"

	"github.com/treeverse/lakefs/forest/tree"

	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/forest/sstable"
)

func TestRemoveDuplicates(t *testing.T) {
	tests := []struct {
		left                 *tree.Tree
		right                *tree.Tree
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
		gotLeft, gotRight := tree.RemoveDuplicates(tst.left, tst.right)
		if len(gotLeft.Parts) != len(tst.expectedLeftPartIds) {
			t.Fatalf("got unexpected number of parts on left tree. expected=%d, got=%d", len(tst.expectedLeftPartIds), len(gotLeft.Parts))
		}
		for i := range gotLeft.Parts {
			if string(gotLeft.Parts[i].Name) != tst.expectedLeftPartIds[i] {
				t.Fatalf("unexpected part id in new left tree index %d: expected=%s, got=%s", i, tst.expectedLeftPartIds[i], string(gotLeft.Parts[i].Name))
			}
		}
		if len(gotRight.Parts) != len(tst.expectedRightPartIds) {
			t.Fatalf("got unexpected number of parts on right tree. expected=%d, got=%d", len(tst.expectedRightPartIds), len(gotRight.Parts))
		}
		for i := range gotRight.Parts {
			if string(gotRight.Parts[i].Name) != tst.expectedRightPartIds[i] {
				t.Fatalf("unexpected part id in new right tree index %d: expected=%s, got=%s", i, tst.expectedRightPartIds[i], string(gotRight.Parts[i].Name))
			}
		}
	}
}

func newTestTree(partIds []string, maxPaths []string) *tree.Tree {
	parts := make([]*tree.Part, 0, len(partIds))
	for i := range partIds {
		parts = append(parts, &tree.Part{
			Name:    sstable.ID(partIds[i]),
			MaxPath: rocks.Path(maxPaths[i]),
		})
	}
	return &tree.Tree{
		Parts: parts,
	}
}
