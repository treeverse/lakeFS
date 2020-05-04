package api

import (
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/index/model"
)

func serializeDiff(d model.Difference) *models.Diff {
	var direction, pathType, diffType string
	switch d.Direction {
	case model.DifferenceDirectionLeft:
		direction = models.DiffDirectionLEFT
	case model.DifferenceDirectionConflict:
		direction = models.DiffDirectionCONFLICT
	case model.DifferenceDirectionRight:
		direction = models.DiffDirectionRIGHT
	}

	switch d.PathType {
	case model.EntryTypeTree:
		pathType = models.DiffPathTypeTREE
	case model.EntryTypeObject:
		pathType = models.DiffPathTypeOBJECT
	}

	switch d.Type {
	case model.DifferenceTypeChanged:
		diffType = models.DiffTypeCHANGED
	case model.DifferenceTypeAdded:
		diffType = models.DiffTypeADDED
	case model.DifferenceTypeRemoved:
		diffType = models.DiffTypeREMOVED
	}

	return &models.Diff{
		Direction: direction,
		Path:      d.Path,
		PathType:  pathType,
		Type:      diffType,
	}
}
