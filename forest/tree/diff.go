package tree

import (
	"github.com/treeverse/lakefs/catalog/rocks"
	"github.com/treeverse/lakefs/forest/sstable"
	"github.com/treeverse/lakefs/graveler"
)

type Part struct {
	Name    sstable.ID `json:"name"`
	MaxPath rocks.Path `json:"max_path"`
}

type Tree struct {
	parts []*Part
}

func compareParts(leftParts []*Part, leftIdx int, rightParts []*Part, rightIdx int) int {
	if leftIdx == len(leftParts) {
		return 1
	}
	if rightIdx == len(rightParts) {
		return -1
	}
	if leftParts[leftIdx].Name == rightParts[rightIdx].Name {
		return 0
	}
	if leftParts[leftIdx].MaxPath < rightParts[rightIdx].MaxPath {
		return -1
	}
	return 1
}

func removeDuplicates(left *Tree, right *Tree) (newLeft *Tree, newRight *Tree) {
	i := 0
	j := 0
	newLeft = new(Tree)
	newRight = new(Tree)
	for i < len(left.parts) || j < len(right.parts) {
		comp := compareParts(left.parts, i, right.parts, j)
		switch comp {
		case 0:
			i++
			j++
		case -1:
			newLeft.parts = append(newLeft.parts, left.parts[i])
			i++
		case 1:
			newRight.parts = append(newRight.parts, right.parts[j])
			j++
		}
	}
	return
}

func Diff(left *Tree, right *Tree, base *Tree) graveler.DiffIterator {
	left, right = removeDuplicates(left, right)

}
