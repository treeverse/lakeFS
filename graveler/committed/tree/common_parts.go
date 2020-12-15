package tree

import (
	"bytes"
	"strings"
)

func compareParts(leftPart Part, rightPart Part) int {
	result := bytes.Compare(leftPart.MaxKey, rightPart.MaxKey)
	if result != 0 {
		return result
	}
	return strings.Compare(string(leftPart.Name), string(rightPart.Name))
}

func RemoveCommonParts(left *Tree, right *Tree) (*Tree, *Tree) {
	i := 0
	j := 0
	newLeft := new(Tree)
	newRight := new(Tree)
	for i < len(left.Parts) && j < len(right.Parts) {
		switch compareParts(left.Parts[i], right.Parts[j]) {
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
	for ; i < len(left.Parts); i++ {
		newLeft.Parts = append(newLeft.Parts, left.Parts[i])
	}
	for ; j < len(right.Parts); j++ {
		newRight.Parts = append(newRight.Parts, right.Parts[j])
	}
	return newLeft, newRight
}
