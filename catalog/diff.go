package catalog

import (
	"fmt"
)

type DifferenceDirection int
type DifferenceType int

const (
	DifferenceDirectionLeft DifferenceDirection = iota
	DifferenceDirectionRight
	DifferenceDirectionConflict

	DifferenceTypeAdded DifferenceType = iota
	DifferenceTypeRemoved
	DifferenceTypeChanged

	EntryTypeObject = "object"
	EntryTypeTree   = "tree"
)

type Difference struct {
	Type      DifferenceType
	Direction DifferenceDirection
	Path      string
	PathType  string
}

func (d Difference) String() string {
	var symbol, direction, pType string
	switch d.Type {
	case DifferenceTypeAdded:
		symbol = "+"
	case DifferenceTypeRemoved:
		symbol = "-"
	case DifferenceTypeChanged:
		symbol = "~"
	}

	switch d.Direction {
	case DifferenceDirectionLeft:
		direction = "<"
	case DifferenceDirectionRight:
		direction = ">"
	case DifferenceDirectionConflict:
		direction = "*"
	}

	switch d.PathType {
	case EntryTypeTree:
		pType = "D"
	case EntryTypeObject:
		pType = "O"
	}

	return fmt.Sprintf("%s%s%s %s", direction, symbol, pType, d.Path)
}

type Differences []Difference
