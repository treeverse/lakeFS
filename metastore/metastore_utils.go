package metastore

import (
	"fmt"
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

type ComparableIterator interface {
	Sort()
	HasMore() bool
	Next()
	GetName() string
	GreaterEqual(b ComparableIterator) bool
	KeyEqual(b ComparableIterator) bool
	ValueEqual(b ComparableIterator) bool
}

func GetDiff(iterA, iterB ComparableIterator) catalog.Differences {
	var diff catalog.Differences
	Diff(iterA, iterB, func(diffType catalog.DifferenceType, iter ComparableIterator) {
		diff = append(diff, catalog.Difference{
			Type: diffType,
			Path: iter.GetName(),
		})
	})
	return diff
}

func Diff(iterA, iterB ComparableIterator, f func(difference catalog.DifferenceType, iter ComparableIterator)) {
	iterA.Sort()
	iterB.Sort()
	for iterA.HasMore() || iterB.HasMore() {
		if !iterA.HasMore() {
			f(catalog.DifferenceTypeRemoved, iterB)
			iterB.Next()
			continue
		}
		if !iterB.HasMore() {
			f(catalog.DifferenceTypeAdded, iterA)
			iterA.Next()
			continue
		}
		if iterA.GreaterEqual(iterB) {
			if iterA.KeyEqual(iterB) {
				if !iterA.ValueEqual(iterB) {
					f(catalog.DifferenceTypeChanged, iterA)
				}
				iterA.Next()
				iterB.Next()
				continue
			}
			f(catalog.DifferenceTypeAdded, iterA)
			iterA.Next()
			continue
		}
		f(catalog.DifferenceTypeRemoved, iterB)
		iterB.Next()
	}
}
