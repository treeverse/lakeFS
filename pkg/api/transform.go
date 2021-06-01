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
	case catalog.DifferenceTypeChanged, catalog.DifferenceTypeCommonPrefix:
		return "changed"
	case catalog.DifferenceTypeConflict:
		return "conflict"
	default:
		return ""
	}
}
