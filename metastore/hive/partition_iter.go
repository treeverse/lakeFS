package hive

import (
	"sort"
	"strings"

	"github.com/treeverse/lakefs/metastore"
	"github.com/treeverse/lakefs/metastore/hive/thrift/gen-go/hive_metastore"
)

type PartitionIter struct {
	partitionList []*hive_metastore.Partition
	current       int
}

func NewPartitionIter(partition []*hive_metastore.Partition) *PartitionIter {
	return &PartitionIter{
		partitionList: partition,
		current:       0,
	}
}

func partitionName(partition *hive_metastore.Partition) string {
	return strings.Join(partition.GetValues(), "-")
}

func comparePartitions(partitionA, partitionB *hive_metastore.Partition) bool {
	return partitionName(partitionA) <= partitionName(partitionB)
}

func (p *PartitionIter) getCurrent() *hive_metastore.Partition {
	return p.partitionList[p.current]
}

func (p *PartitionIter) Sort() {
	partitions := p.partitionList
	sort.Slice(partitions, func(i, j int) bool {
		return comparePartitions(partitions[i], partitions[j])
	})
	p.partitionList = partitions
}

func (p *PartitionIter) HasMore() bool {
	return len(p.partitionList) > p.current
}

func (p *PartitionIter) Next() {
	p.current++
}

func (p *PartitionIter) GetName() string {
	return partitionName(p.getCurrent())
}

func (p *PartitionIter) GreaterEqual(b metastore.ComparableIterator) bool {
	return p.GetName() <= b.GetName()
}

func (p *PartitionIter) KeyEqual(b metastore.ComparableIterator) bool {
	return p.GetName() == b.GetName()
}

func (p *PartitionIter) ValueEqual(b metastore.ComparableIterator) bool {
	pCur := p.getCurrent()
	bCur := b.(*PartitionIter).getCurrent()
	return pCur.LastAccessTime == bCur.LastAccessTime && len(pCur.GetSd().GetCols()) == len(bCur.GetSd().GetCols())
}
