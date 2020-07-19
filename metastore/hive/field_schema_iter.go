package hive

import (
	"fmt"

	"github.com/treeverse/lakefs/metastore"
	"github.com/treeverse/lakefs/metastore/hive/gen-go/hive_metastore"
)

type FSIter struct {
	fsList []*hive_metastore.FieldSchema
}

func (p *FSIter) Name(i int) string {
	return p.fsList[i].GetName()
}

func (p *FSIter) Len() int {
	return len(p.fsList)
}

func (p *FSIter) Less(i, j int) bool {
	return compareFieldSchema(p.fsList[i], p.fsList[j]) == metastore.ItemLess
}

func (p *FSIter) Swap(i, j int) {
	p.fsList[i], p.fsList[j] = p.fsList[j], p.fsList[i]
}

func (p *FSIter) Value(i int) interface{} {
	return p.fsList[i]
}

func (p *FSIter) CompareWith(i int, v interface{}, j int) metastore.CompareResult {
	if partition, ok := v.(*FSIter); ok {
		return compareFieldSchema(p.fsList[i], partition.fsList[j])
	}
	msg := fmt.Sprintf("expected to get value of type *hive.FSIter gor: %T", v)
	panic(msg)
}
func compareFieldSchema(fsA, fsB *hive_metastore.FieldSchema) metastore.CompareResult {
	if fsA.GetName() < fsB.GetName() {
		return metastore.ItemLess
	} else if fsA.GetName() == fsB.GetName() {
		if FieldSchemaEqual(fsA, fsB) {
			return metastore.ItemSame
		}
		return metastore.ItemSameKey
	}
	return metastore.ItemGreater
}
func NewFSIter(fsList []*hive_metastore.FieldSchema) *FSIter {
	return &FSIter{
		fsList: fsList,
	}
}

func FieldSchemaEqual(f1, f2 *hive_metastore.FieldSchema) bool {
	return f1.GetType() == f2.GetType() && f1.GetComment() == f2.GetComment() && f1.GetName() == f2.GetName()
}
