package metastore

import (
	"fmt"
	"sort"
	"strings"

	"github.com/treeverse/lakefs/catalog"
)

const SymlinkInputFormat = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"

func TransformLocation(location, branch, branch2 string) string {
	return strings.Replace(location, fmt.Sprintf("/%s/", branch), fmt.Sprintf("/%s/", branch2), 1)
}

func GetSymlinkLocation(location, locationPrefix string) string {
	location = strings.TrimSuffix(location, "/")
	locationPrefix = strings.TrimSuffix(locationPrefix, "/")

	locationParts := strings.Split(location, "/")
	prefixParts := strings.Split(locationPrefix, "/")

	target := prefixParts[len(prefixParts)-1]
	var origLocationPrefix string
	for i := len(locationParts) - 1; i >= 0; i-- {
		if locationParts[i] == target {
			origLocationPrefix = strings.Join(locationParts[:i+1], "/")
			break
		}
	}
	res := strings.Replace(location, origLocationPrefix, locationPrefix, 1)
	return res
}

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

type ComparableIterator interface {
	sort.Interface
	Value(i int) interface{}
	Name(i int) string
	CompareWith(i int, v interface{}, j int) CompareResult
}

func DiffIterable(iterA, iterB ComparableIterator, f func(difference catalog.DifferenceType, iter interface{}, name string)) {
	sort.Sort(iterA)
	sort.Sort(iterB)

	for i, j, sizeA, sizeB := 0, 0, iterA.Len(), iterB.Len(); i < sizeA && j < sizeB; {
		if i >= sizeA {
			f(catalog.DifferenceTypeRemoved, iterB.Value(j), iterB.Name(j))
			j++
			continue
		}
		if j >= sizeB {
			f(catalog.DifferenceTypeAdded, iterA.Value(i), iterA.Name(i))
			i++
			continue
		}
		switch iterA.CompareWith(i, iterB, j) {
		case ItemSameKey:
			f(catalog.DifferenceTypeChanged, iterA.Value(i), iterA.Name(i))
			i++
			j++

		case ItemSame:
			i++
			j++

		case ItemGreater:
			f(catalog.DifferenceTypeRemoved, iterB.Value(j), iterB.Name(j))
			j++
		case ItemLess:
			f(catalog.DifferenceTypeAdded, iterA.Value(i), iterA.Name(i))
			i++
		}
	}
}

func Diff(iterA, iterB ComparableIterator) catalog.Differences {
	var diff catalog.Differences
	DiffIterable(iterA, iterB, func(diffType catalog.DifferenceType, _ interface{}, name string) {
		diff = append(diff, catalog.Difference{
			Type: diffType,
			Path: name,
		})
	})
	return diff
}
