package metastore

import (
	"sort"

	"github.com/treeverse/lakefs/catalog"
)

type MetaDiff struct {
	PartitionDiff catalog.Differences
	ColumnsDiff   catalog.Differences
}

type CompareResult int

const (
	ItemLess CompareResult = iota
	ItemSameKey
	ItemSame
	ItemGreater
)

type Collection interface {
	sort.Interface
	Value(i int) interface{}
	Name(i int) string
	CompareWith(i int, v interface{}, j int) CompareResult
}

type DiffCallbackFn func(difference catalog.DifferenceType, iter interface{}, name string) error

func DiffIterable(iterA, iterB Collection, callbackFn DiffCallbackFn) error {
	sort.Sort(iterA)
	sort.Sort(iterB)
	i, j, sizeA, sizeB := 0, 0, iterA.Len(), iterB.Len()
	for i < sizeA && j < sizeB {
		switch iterA.CompareWith(i, iterB, j) {
		case ItemSameKey:
			err := callbackFn(catalog.DifferenceTypeChanged, iterA.Value(i), iterA.Name(i))
			if err != nil {
				return err
			}
			i++
			j++

		case ItemSame:
			i++
			j++

		case ItemGreater:
			err := callbackFn(catalog.DifferenceTypeRemoved, iterB.Value(j), iterB.Name(j))
			if err != nil {
				return err
			}
			j++

		case ItemLess:
			err := callbackFn(catalog.DifferenceTypeAdded, iterA.Value(i), iterA.Name(i))
			if err != nil {
				return err
			}
			i++
		}
	}

	for j < sizeB {
		err := callbackFn(catalog.DifferenceTypeRemoved, iterB.Value(j), iterB.Name(j))
		if err != nil {
			return err
		}
		j++
	}
	for i < sizeA {
		err := callbackFn(catalog.DifferenceTypeAdded, iterA.Value(i), iterA.Name(i))
		if err != nil {
			return err
		}
		i++
	}
	return nil
}

func Diff(iterA, iterB Collection) (catalog.Differences, error) {
	var diff catalog.Differences
	err := DiffIterable(iterA, iterB, func(diffType catalog.DifferenceType, _ interface{}, name string) error {
		diff = append(diff, catalog.Difference{
			Type:  diffType,
			Entry: catalog.Entry{Path: name},
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return diff, nil
}
