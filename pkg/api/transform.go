package api

import (
	"strings"

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
	default:
		return ""
	}
}

func transformDifferenceToDiff(difference catalog.Difference) *Diff {
	d := &Diff{
		Path: StringPtr(difference.Path),
		Type: StringPtr(transformDifferenceTypeToString(difference.Type)),
	}
	if strings.HasSuffix(difference.Path, catalog.DefaultPathDelimiter) {
		d.PathType = StringPtr("common_prefix")
	} else {
		d.PathType = StringPtr("object")
	}
	return d
}
