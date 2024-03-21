package api

import (
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/catalog"
)

func transformDifferenceTypeToString(d catalog.DifferenceType) apigen.DiffType {
	switch d {
	case catalog.DifferenceTypeAdded:
		return apigen.DiffTypeAdded
	case catalog.DifferenceTypeRemoved:
		return apigen.DiffTypeRemoved
	case catalog.DifferenceTypeChanged:
		return apigen.DiffTypeChanged
	case catalog.DifferenceTypeConflict:
		return apigen.DiffTypeConflict
	case catalog.DifferenceTypePrefixChanged:
		return apigen.DiffTypePrefixChanged
	default:
		return ""
	}
}
