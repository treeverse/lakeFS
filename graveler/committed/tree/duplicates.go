package tree

import "bytes"

type treeRepo struct {
}

func compareParts(leftParts []Part, leftIdx int, rightParts []Part, rightIdx int) int {
	if leftIdx == len(leftParts) {
		return 1
	}
	if rightIdx == len(rightParts) {
		return -1
	}
	if leftParts[leftIdx].Name == rightParts[rightIdx].Name {
		return 0
	}
	return bytes.Compare(leftParts[leftIdx].MaxKey, rightParts[rightIdx].MaxKey)
}

func (tr *treeRepo) RemoveCommonParts(left *Tree, right *Tree) (newLeft *Tree, newRight *Tree) {
	i := 0
	j := 0
	newLeft = new(Tree)
	newRight = new(Tree)
	for i < len(left.Parts) || j < len(right.Parts) {
		comp := compareParts(left.Parts, i, right.Parts, j)
		switch comp {
		case 0:
			i++
			j++
		case -1:
			newLeft.Parts = append(newLeft.Parts, left.Parts[i])
			i++
		case 1:
			newRight.Parts = append(newRight.Parts, right.Parts[j])
			j++
		}
	}
	return
}
