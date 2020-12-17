package tree

import (
	"bytes"
)

func RemoveCommonParts(left *Tree, right *Tree) (*Tree, *Tree) {
	i := 0
	j := 0
	newLeft := new(Tree)
	newRight := new(Tree)
	for i < len(left.Parts) && j < len(right.Parts) {
		switch bytes.Compare(left.Parts[i].MaxKey, right.Parts[j].MaxKey) {
		case 0:
			if string(left.Parts[i].Name) != string(right.Parts[j].Name) {
				i++
				j++
			}
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
