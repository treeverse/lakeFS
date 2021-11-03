package metastore

import (
	"fmt"

	mserrors "github.com/treeverse/lakefs/pkg/metastore/errors"
)

type ColumnCollection struct {
	columns []*FieldSchema
}

func (c *ColumnCollection) Name(i int) string {
	return c.columns[i].Name
}

func (c *ColumnCollection) Len() int {
	return len(c.columns)
}

func (c *ColumnCollection) Less(i, j int) bool {
	return compareColumns(c.columns[i], c.columns[j]) == ItemLess
}

func (c *ColumnCollection) Swap(i, j int) {
	c.columns[i], c.columns[j] = c.columns[j], c.columns[i]
}

func (c *ColumnCollection) Value(i int) interface{} {
	return c.columns[i]
}

func (c *ColumnCollection) CompareWith(i int, v interface{}, j int) CompareResult {
	if otherIter, ok := v.(*ColumnCollection); ok {
		return compareColumns(c.columns[i], otherIter.columns[j])
	}
	err := fmt.Errorf("%w ColumnCollection, got %T", mserrors.ErrExpectedType, v)
	panic(err)
}

func compareColumns(columnA, columnB *FieldSchema) CompareResult {
	nameA, nameB := columnA.Name, columnB.Name
	if nameA < nameB {
		return ItemLess
	}
	if nameA > nameB {
		return ItemGreater
	}
	if columnA.Type == columnB.Type && columnA.Comment == columnB.Comment {
		return ItemSame
	}
	return ItemSameKey
}

func NewColumnCollection(columns []*FieldSchema) *ColumnCollection {
	return &ColumnCollection{
		columns: columns,
	}
}
