package glue

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/treeverse/lakefs/metastore"
)

type ColumnIter struct {
	columns []*glue.Column
	current int
}

func (c *ColumnIter) getCurrent() *glue.Column {
	return c.columns[c.current]
}

func (c *ColumnIter) Sort() {
	// do nothing
}

func (c *ColumnIter) HasMore() bool {
	return len(c.columns) > c.current
}

func (c *ColumnIter) Next() {
	c.current++
}

func (c *ColumnIter) GetName() string {
	return aws.StringValue(c.getCurrent().Name)
}

func (c *ColumnIter) GreaterEqual(other metastore.ComparableIterator) bool {
	return c.GetName() <= other.GetName()
}

func (c *ColumnIter) KeyEqual(other metastore.ComparableIterator) bool {
	return c.GetName() == other.GetName()
}

func ColumnEqual(column, otherColumn *glue.Column) bool {
	return aws.StringValue(column.Name) == aws.StringValue(otherColumn.Name) && aws.StringValue(column.Type) == aws.StringValue(otherColumn.Type) && aws.StringValue(column.Comment) == aws.StringValue(otherColumn.Comment)
}
func (c *ColumnIter) ValueEqual(other metastore.ComparableIterator) bool {
	column := c.getCurrent()
	otherColumn := other.(*ColumnIter).getCurrent()
	return ColumnEqual(column, otherColumn)
}

func NewColumnIter(columns []*glue.Column) *ColumnIter {
	return &ColumnIter{
		columns: columns,
		current: 0,
	}
}
