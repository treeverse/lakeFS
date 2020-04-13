package api

import (
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/index/merkle"
	"github.com/treeverse/lakefs/index/model"
)

func serializeDiff(d merkle.Difference) *models.Diff {
	var direction, pathType, diffType string
	switch d.Direction {
	case merkle.DifferenceDirectionLeft:
		direction = models.DiffDirectionLEFT
	case merkle.DifferenceDirectionConflict:
		direction = models.DiffDirectionCONFLICT
	case merkle.DifferenceDirectionRight:
		direction = models.DiffDirectionRIGHT
	}

	switch d.PathType {
	case model.EntryTypeTree:
		pathType = models.DiffPathTypeTREE
	case model.EntryTypeObject:
		pathType = models.DiffPathTypeOBJECT
	}

	switch d.Type {
	case merkle.DifferenceTypeChanged:
		diffType = models.DiffTypeCHANGED
	case merkle.DifferenceTypeAdded:
		diffType = models.DiffTypeADDED
	case merkle.DifferenceTypeRemoved:
		diffType = models.DiffTypeREMOVED
	}

	return &models.Diff{
		Direction: direction,
		Path:      d.Path,
		PathType:  pathType,
		Type:      diffType,
	}
}
