package glue

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/treeverse/lakefs/pkg/metastore"
)

type ColumnCollection struct {
	columns []*glue.Column
}

func (c *ColumnCollection) Name(i int) string {
	return aws.StringValue(c.columns[i].Name)
}

func (c *ColumnCollection) Len() int {
	return len(c.columns)
}

func (c *ColumnCollection) Less(i, j int) bool {
	return compareColumns(c.columns[i], c.columns[j]) == metastore.ItemLess
}

func (c *ColumnCollection) Swap(i, j int) {
	c.columns[i], c.columns[j] = c.columns[j], c.columns[i]
}

func (c *ColumnCollection) Value(i int) interface{} {
	return c.columns[i]
}

func (c *ColumnCollection) CompareWith(i int, v interface{}, j int) metastore.CompareResult {
	if otherIter, ok := v.(*ColumnCollection); ok {
		return compareColumns(c.columns[i], otherIter.columns[j])
	}
	err := fmt.Errorf("%w *glue.ColumnCollection, got %T", ErrExpectedType, v)
	panic(err)
}

func compareColumns(columnA, columnB *glue.Column) metastore.CompareResult {
	nameA, nameB := aws.StringValue(columnA.Name), aws.StringValue(columnB.Name)
	if nameA < nameB {
		return metastore.ItemLess
	}
	if nameA > nameB {
		return metastore.ItemGreater
	}
	if ColumnEqual(columnA, columnB) {
		return metastore.ItemSame
	}
	return metastore.ItemSameKey
}

func ColumnEqual(column, otherColumn *glue.Column) bool {
	return aws.StringValue(column.Name) == aws.StringValue(otherColumn.Name) && aws.StringValue(column.Type) == aws.StringValue(otherColumn.Type) && aws.StringValue(column.Comment) == aws.StringValue(otherColumn.Comment)
}

func NewColumnCollection(columns []*glue.Column) *ColumnCollection {
	return &ColumnCollection{
		columns: columns,
	}
}
