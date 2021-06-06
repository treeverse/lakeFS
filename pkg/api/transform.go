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
		// note that common prefixes are always considered "changed".
		// while technically possible to check if the underlying diff would result in the prefix being deleted,
		// this would turn the diff to be O(N) where N is the amount of entries starting with that prefix
		return "changed"
	case catalog.DifferenceTypeConflict:
		return "conflict"
	default:
		return ""
	}
}
