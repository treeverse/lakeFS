package hive

import (
	"sort"

	"github.com/treeverse/lakefs/metastore"
	"github.com/treeverse/lakefs/metastore/hive/thrift/gen-go/hive_metastore"
)

type FSIter struct {
	fsList  []*hive_metastore.FieldSchema
	current int
}

func NewFSIter(fsList []*hive_metastore.FieldSchema) *FSIter {
	return &FSIter{
		fsList:  fsList,
		current: 0,
	}
}

func (p FSIter) getCurrent() *hive_metastore.FieldSchema {
	return p.fsList[p.current]
}

func (p FSIter) Sort() {
	fsList := p.fsList
	sort.Slice(fsList, func(i, j int) bool {
		return fsList[i].GetName() <= fsList[j].GetName()
	})
	p.fsList = fsList //todo don't think we need this line
}

func (p *FSIter) HasMore() bool {
	return len(p.fsList) > p.current
}

func (p *FSIter) Next() {
	p.current++
}

func (p *FSIter) GetName() string {
	return p.getCurrent().GetName()
}

func (p *FSIter) GreaterEqual(b metastore.ComparableIterator) bool {
	return p.GetName() <= b.GetName()
}

func (p *FSIter) KeyEqual(b metastore.ComparableIterator) bool {
	return p.GetName() == b.GetName()
}

func (p *FSIter) ValueEqual(b metastore.ComparableIterator) bool {
	pCur := p.getCurrent()
	bCur := b.(*FSIter).getCurrent()
	return pCur.GetType() == bCur.GetType() && pCur.GetComment() == bCur.GetComment() && pCur.GetName() == bCur.GetName()
}
