package merkle

import (
	"fmt"
	"sort"
	"strings"

	"github.com/treeverse/lakefs/index/model"

	"github.com/treeverse/lakefs/db"
	"golang.org/x/xerrors"

	"github.com/treeverse/lakefs/index/path"
)

type DifferenceDirection int
type DifferenceType int

const (
	DifferenceDirectionLeft DifferenceDirection = iota
	DifferenceDirectionRight
	DifferenceDirectionConflict

	DifferenceTypeAdded DifferenceType = iota
	DifferenceTypeRemoved
	DifferenceTypeChanged
)

type Difference struct {
	Type      DifferenceType
	Direction DifferenceDirection
	Path      string
	PathType  model.Entry_Type
}

func (d Difference) String() string {
	var symbol, direction, pType string
	switch d.Type {
	case DifferenceTypeAdded:
		symbol = "+"
	case DifferenceTypeRemoved:
		symbol = "-"
	case DifferenceTypeChanged:
		symbol = "~"
	}

	switch d.Direction {
	case DifferenceDirectionLeft:
		direction = "<"
	case DifferenceDirectionRight:
		direction = ">"
	case DifferenceDirectionConflict:
		direction = "*"
	}

	switch d.PathType {
	case model.Entry_TREE:
		pType = "D"
	case model.Entry_OBJECT:
		pType = "O"
	}

	return fmt.Sprintf("%s%s%s %s", direction, symbol, pType, d.Path)
}

type Differences []Difference

func diff(tx TreeReader, pth string, left, right, common *Merkle) (Differences, error) {
	// start walking both trees
	// when we notice a change, compare it with common to decide type and direction
	res := make(Differences, 0)
	leftEntries, err := left.GetEntries(tx, pth) // start with root
	if err != nil {
		return nil, err
	}
	rightEntries, err := right.GetEntries(tx, pth) // start with root
	if err != nil {
		return nil, err
	}

	var sentinel = struct{}{}
	visitedNames := make(map[string]struct{})
	for _, leftEntry := range leftEntries {
		entryPath := leftEntry.GetName()
		if len(pth) > 0 {
			entryPath = path.Join([]string{pth, leftEntry.GetName()})
		}

		// see if this tree exists on right
		// if it does, see if checksum is the same - if it is, next
		// if it's different, check common
		// 		if common is the same as left, right modified
		//		if common is the same as right, left modified
		// 		if common is different from both, conflict!
		// if it doesn't exist:
		// 		if it doesn't exist on common as well, left created
		//		if it exists in common, right deleted
		rightIndex := sort.Search(len(rightEntries), func(i int) bool {
			return strings.Compare(rightEntries[i].GetName(), leftEntry.GetName()) >= 0
		})
		if rightIndex < len(rightEntries) && strings.EqualFold(rightEntries[rightIndex].GetName(), leftEntry.GetName()) {
			// we have such a node on the right as well, let's compare it!
			rightEntry := rightEntries[rightIndex]
			if strings.EqualFold(leftEntry.GetAddress(), rightEntry.GetAddress()) {
				// same, move on
				visitedNames[leftEntry.GetName()] = sentinel
				continue
			}

			// not the same as on the right, let's see whose change this is
			commonEntry, err := common.GetEntry(tx, entryPath, leftEntry.GetType())
			if xerrors.Is(err, db.ErrNotFound) {
				// doesn't exist in common but left and right are different
				// this means both trees created it differently, meaning a conflict
				res = append(res, Difference{
					Type:      DifferenceTypeChanged,
					Direction: DifferenceDirectionConflict,
					Path:      entryPath,
					PathType:  leftEntry.GetType(),
				})
				visitedNames[leftEntry.GetName()] = sentinel
				continue
			} else if err != nil {
				return nil, err
			}
			var direction DifferenceDirection
			if !strings.EqualFold(commonEntry.GetAddress(), leftEntry.GetAddress()) && !strings.EqualFold(commonEntry.GetAddress(), rightEntry.GetAddress()) {
				// conflict
				direction = DifferenceDirectionConflict
			} else if strings.EqualFold(commonEntry.GetAddress(), leftEntry.GetAddress()) {
				// right made change
				// direction = DifferenceDirectionRight
				// WE DON'T CARE ABOUT RIGHT MODIFICATIONS
				visitedNames[leftEntry.GetName()] = sentinel
				continue
			} else {
				// left made change
				direction = DifferenceDirectionLeft
			}
			res = append(res, Difference{
				Type:      DifferenceTypeChanged,
				Direction: direction,
				Path:      entryPath,
				PathType:  leftEntry.GetType(),
			})
			visitedNames[leftEntry.GetName()] = sentinel
			continue
		}

		// this node doesn't exist on right, so it was either deleted right or created left,
		// let's use common to test
		commonEntry, err := common.GetEntry(tx, entryPath, leftEntry.GetType())
		if xerrors.Is(err, db.ErrNotFound) {
			// exists only on left
			res = append(res, Difference{
				Type:      DifferenceTypeAdded,
				Direction: DifferenceDirectionLeft,
				Path:      entryPath,
				PathType:  leftEntry.GetType(),
			})
			visitedNames[leftEntry.GetName()] = sentinel
			continue
		} else if err != nil {
			return nil, err
		}

		// exists on left, exists on common, doesn't exist on right
		if strings.EqualFold(leftEntry.GetAddress(), commonEntry.GetAddress()) {
			// if left and common are the same, right deleted
			// WE DON'T CARE ABOUT RIGHT MODIFICATIONS
			visitedNames[leftEntry.GetName()] = sentinel
			continue
		}
		// if left and common are different: left modified while right deleted - conflict
		res = append(res, Difference{
			Type:      DifferenceTypeChanged,
			Direction: DifferenceDirectionConflict,
			Path:      entryPath,
			PathType:  leftEntry.GetType(),
		})
		visitedNames[leftEntry.GetName()] = sentinel
		continue
	}

	for _, rightEntry := range rightEntries {
		if _, visited := visitedNames[rightEntry.GetName()]; visited {
			continue
		}
		// this node doesn't exist on the left, it was either deleted left or created right
		// let's use common to test
		entryPath := rightEntry.GetName()
		if len(pth) > 0 {
			entryPath = path.Join([]string{pth, rightEntry.GetName()})
		}
		commonEntry, err := common.GetEntry(tx, entryPath, rightEntry.GetType())
		if xerrors.Is(err, db.ErrNotFound) {
			// doesn't exist left, doesn't exist common - right created
			// we don't currently record right modifications unless they are conflicting
			continue
		} else if err != nil {
			return nil, err
		}

		if strings.EqualFold(rightEntry.GetAddress(), commonEntry.GetAddress()) {
			// right and common are equal,left doesnt exist - left deleted
			res = append(res, Difference{
				Type:      DifferenceTypeRemoved,
				Direction: DifferenceDirectionLeft,
				Path:      entryPath,
				PathType:  rightEntry.GetType(),
			})
			continue
		}
		// right and common are different, left doesn't exist - right modified and left deleted = conflict
		res = append(res, Difference{
			Type:      DifferenceTypeRemoved,
			Direction: DifferenceDirectionConflict,
			Path:      entryPath,
			PathType:  rightEntry.GetType(),
		})
	}
	return res, nil
}

type diffCollector struct {
	results Differences
}

func diffWalk(tx TreeReader, pth string, left, right, common *Merkle, collector *diffCollector) error {
	results, err := diff(tx, pth, left, right, common)
	if err != nil {
		return err
	}
	for _, current := range results {
		// if we get a "conflicting" directory, drill down into it
		if current.PathType == model.Entry_TREE && (current.Direction == DifferenceDirectionConflict || current.Type == DifferenceTypeChanged) {
			err = diffWalk(tx, current.Path, left, right, common, collector)
			if err != nil {
				return err
			}
			continue
		}
		collector.results = append(collector.results, current)
	}
	return nil
}

func Diff(tx TreeReader, left, right, common *Merkle) (Differences, error) {
	totalDiff := &diffCollector{results: make(Differences, 0)}
	err := diffWalk(tx, "", left, right, common, totalDiff)
	if err != nil {
		return nil, err
	}
	return totalDiff.results, nil
}
