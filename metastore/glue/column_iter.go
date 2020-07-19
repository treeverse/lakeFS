package glue

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/treeverse/lakefs/metastore"
)

type ColumnIter struct {
	columns []*glue.Column
}

func (c *ColumnIter) Name(i int) string {
	return aws.StringValue(c.columns[i].Name)
}

func (c *ColumnIter) Len() int {
	return len(c.columns)
}

func (c *ColumnIter) Less(i, j int) bool {
	return compareColumns(c.columns[i], c.columns[j]) == metastore.ItemLess
}

func (c *ColumnIter) Swap(i, j int) {
	c.columns[i], c.columns[j] = c.columns[j], c.columns[i]
}

func (c *ColumnIter) Value(i int) interface{} {
	return c.columns[i]
}

func (c *ColumnIter) CompareWith(i int, v interface{}, j int) metastore.CompareResult {
	if otherIter, ok := v.(*ColumnIter); ok {
		return compareColumns(c.columns[i], otherIter.columns[j])
	}
	msg := fmt.Sprintf("expected type *glue.ColumnIter, got %T.", v)
	panic(msg)
}

func compareColumns(columnA, columnB *glue.Column) metastore.CompareResult {
	nameA, nameB := aws.StringValue(columnA.Name), aws.StringValue(columnB.Name)
	if nameA < nameB {
		return metastore.ItemLess
	} else if nameA == nameB {
		if ColumnEqual(columnA, columnB) {
			return metastore.ItemSame
		}
		return metastore.ItemSameKey
	}
	return metastore.ItemGreater
}

func ColumnEqual(column, otherColumn *glue.Column) bool {
	return aws.StringValue(column.Name) == aws.StringValue(otherColumn.Name) && aws.StringValue(column.Type) == aws.StringValue(otherColumn.Type) && aws.StringValue(column.Comment) == aws.StringValue(otherColumn.Comment)
}

func NewColumnIter(columns []*glue.Column) *ColumnIter {
	return &ColumnIter{
		columns: columns,
	}
}
