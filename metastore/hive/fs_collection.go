package hive

import (
	"fmt"

	"github.com/treeverse/lakefs/metastore"
	"github.com/treeverse/lakefs/metastore/hive/gen-go/hive_metastore"
)

type FSCollection struct {
	fsList []*hive_metastore.FieldSchema
}

func (p *FSCollection) Name(i int) string {
	return p.fsList[i].GetName()
}

func (p *FSCollection) Len() int {
	return len(p.fsList)
}

func (p *FSCollection) Less(i, j int) bool {
	return compareFieldSchema(p.fsList[i], p.fsList[j]) == metastore.ItemLess
}

func (p *FSCollection) Swap(i, j int) {
	p.fsList[i], p.fsList[j] = p.fsList[j], p.fsList[i]
}

func (p *FSCollection) Value(i int) interface{} {
	return p.fsList[i]
}

func (p *FSCollection) CompareWith(i int, v interface{}, j int) metastore.CompareResult {
	if partition, ok := v.(*FSCollection); ok {
		return compareFieldSchema(p.fsList[i], partition.fsList[j])
	}
	err := fmt.Errorf("%w *hive.FSCollection got: %T", ErrExpectedType, v)
	panic(err)
}

func compareFieldSchema(fsA, fsB *hive_metastore.FieldSchema) metastore.CompareResult {
	if fsA.GetName() < fsB.GetName() {
		return metastore.ItemLess
	}
	if fsA.GetName() > fsB.GetName() {
		return metastore.ItemGreater
	}
	if FieldSchemaEqual(fsA, fsB) {
		return metastore.ItemSame
	}
	return metastore.ItemSameKey
}

func NewFSCollection(fsList []*hive_metastore.FieldSchema) *FSCollection {
	return &FSCollection{
		fsList: fsList,
	}
}

func FieldSchemaEqual(f1, f2 *hive_metastore.FieldSchema) bool {
	return f1.GetType() == f2.GetType() && f1.GetComment() == f2.GetComment() && f1.GetName() == f2.GetName()
}
