package tree_test

import (
	"testing"

	"github.com/go-test/deep"

	"github.com/treeverse/lakefs/graveler"
	"github.com/treeverse/lakefs/graveler/committed/sstable"
	"github.com/treeverse/lakefs/graveler/committed/tree"
)

func TestRemoveCommonParts(t *testing.T) {
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
		gotLeft, gotRight := tree.RemoveCommonParts(tst.left, tst.right)
		if diff := deep.Equal(tst.expectedLeftPartIds, partNames(gotLeft)); diff != nil {
			t.Fatalf("got unexpected parts on left tree. diff expected-got: %s", diff)
		}
		if diff := deep.Equal(tst.expectedRightPartIds, partNames(gotRight)); diff != nil {
			t.Fatalf("got unexpected parts on right tree. diff expected-got: %s", diff)
		}
	}
}

func partNames(tree *tree.Tree) []string {
	names := make([]string, 0, len(tree.Parts))
	for _, p := range tree.Parts {
		names = append(names, string(p.Name))
	}
	return names
}

func newTestTree(partIds []string, maxPaths []string) *tree.Tree {
	parts := make([]tree.Part, 0, len(partIds))
	for i := range partIds {
		parts = append(parts, tree.Part{
			Name:   sstable.ID(partIds[i]),
			MaxKey: graveler.Key(maxPaths[i]),
		})
	}
	return &tree.Tree{
		Parts: parts,
	}
}
