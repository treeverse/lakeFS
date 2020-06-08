package catalog

import (
	"fmt"
)

type DifferenceType int

const (
	DifferenceTypeAdded DifferenceType = iota
	DifferenceTypeRemoved
	DifferenceTypeChanged
	DifferenceTypeConflict
)

type Difference struct {
	Type DifferenceType `db:"diff_type"`
	Path string
}

func (d Difference) String() string {
	var symbol string
	switch d.Type {
	case DifferenceTypeAdded:
		symbol = "+"
	case DifferenceTypeRemoved:
		symbol = "-"
	case DifferenceTypeChanged:
		symbol = "~"
	case DifferenceTypeConflict:
		symbol = "x"
	}

	return fmt.Sprintf("%s %s", symbol, d.Path)
}

type Differences []Difference
