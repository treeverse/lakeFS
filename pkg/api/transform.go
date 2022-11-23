package api

import (
	"github.com/treeverse/lakefs/pkg/catalog"
)

func transformDifferenceTypeToString(d catalog.DifferenceType) string {
	switch d {
	case catalog.DifferenceTypeAdded:
		return "added"
	case catalog.DifferenceTypeRemoved:
		return "removed"
	case catalog.DifferenceTypeChanged:
		return "changed"
	case catalog.DifferenceTypeConflict:
		return "conflict"
	case catalog.DifferenceTypePrefixChanged:
		return "prefix_changed"
	default:
		return ""
	}
}
