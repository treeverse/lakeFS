package api

import (
	"strings"

	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/catalog"
)

func transformDifferenceToMergeResult(difference catalog.Difference) *models.MergeResult {
	mr := &models.MergeResult{
		Path: difference.Path,
	}
	switch difference.Type {
	case catalog.DifferenceTypeAdded:
		mr.Type = models.MergeResultTypeAdded
	case catalog.DifferenceTypeRemoved:
		mr.Type = models.MergeResultTypeRemoved
	case catalog.DifferenceTypeChanged:
		mr.Type = models.MergeResultTypeChanged
	case catalog.DifferenceTypeConflict:
		mr.Type = models.MergeResultTypeConflict
	}

	if strings.HasSuffix(difference.Path, catalog.DefaultPathDelimiter) {
		mr.PathType = models.MergeResultPathTypeCommonPrefix
	} else {
		mr.PathType = models.MergeResultPathTypeObject
	}
	return mr
}

func transformDifferenceToDiff(difference catalog.Difference) *models.Diff {
	d := &models.Diff{
		Path: difference.Path,
	}
	switch difference.Type {
	case catalog.DifferenceTypeAdded:
		d.Type = models.DiffTypeAdded
	case catalog.DifferenceTypeRemoved:
		d.Type = models.DiffTypeRemoved
	case catalog.DifferenceTypeChanged:
		d.Type = models.DiffTypeChanged
	case catalog.DifferenceTypeConflict:
		d.Type = models.DiffTypeConflict
	}

	if strings.HasSuffix(difference.Path, catalog.DefaultPathDelimiter) {
		d.PathType = models.DiffPathTypeCommonPrefix
	} else {
		d.PathType = models.DiffPathTypeObject
	}
	return d
}
