package api

import (
	"strings"

	"github.com/treeverse/lakefs/pkg/api/gen/models"
	"github.com/treeverse/lakefs/pkg/catalog"
)

func transformDifferenceTypeToString(d catalog.DifferenceType) string {
	switch d {
	case catalog.DifferenceTypeAdded:
		return models.DiffTypeAdded
	case catalog.DifferenceTypeRemoved:
		return models.DiffTypeRemoved
	case catalog.DifferenceTypeChanged:
		return models.DiffTypeChanged
	case catalog.DifferenceTypeConflict:
		return models.DiffTypeConflict
	default:
		return ""
	}
}

func transformDifferenceToDiff(difference catalog.Difference) *models.Diff {
	d := &models.Diff{
		Path: difference.Path,
	}
	d.Type = transformDifferenceTypeToString(difference.Type)
	if strings.HasSuffix(difference.Path, catalog.DefaultPathDelimiter) {
		d.PathType = models.DiffPathTypeCommonPrefix
	} else {
		d.PathType = models.DiffPathTypeObject
	}
	return d
}
