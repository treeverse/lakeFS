package hive

import (
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/metastore"
	"github.com/treeverse/lakefs/metastore/hive/gen-go/hive_metastore"
)

type PartitionIter struct {
	partitionList []*hive_metastore.Partition
}

func (p *PartitionIter) Name(i int) string {
	return partitionName(p.partitionList[i])
}

func (p *PartitionIter) Value(i int) interface{} {
	return p.partitionList[i]
}

func (p *PartitionIter) CompareWith(i int, v interface{}, j int) metastore.CompareResult {
	if partition, ok := v.(*PartitionIter); ok {
		return compare(p.partitionList[i], partition.partitionList[j])
	}
	msg := fmt.Sprintf("expected to get value of type *hive.PartitionIter gor:%T", v)
	panic(msg)
}

func (p *PartitionIter) Len() int {
	return len(p.partitionList)
}

func valueEqual(a, b *hive_metastore.Partition) bool {
	return a.LastAccessTime == b.LastAccessTime && len(a.GetSd().GetCols()) == len(b.GetSd().GetCols())
}

func compare(partitionA, partitionB *hive_metastore.Partition) metastore.CompareResult {
	if partitionName(partitionA) < partitionName(partitionB) {
		return metastore.ItemLess
	} else if partitionName(partitionA) == partitionName(partitionB) {
		if valueEqual(partitionA, partitionB) {
			return metastore.ItemSame
		}
		return metastore.ItemSameKey
	}
	return metastore.ItemGreater
}

func (p *PartitionIter) Less(i, j int) bool {
	return compare(p.partitionList[i], p.partitionList[j]) == metastore.ItemLess
}

func (p *PartitionIter) Swap(i, j int) {
	p.partitionList[i], p.partitionList[j] = p.partitionList[j], p.partitionList[i]
}

func NewPartitionIter(partition []*hive_metastore.Partition) *PartitionIter {
	return &PartitionIter{
		partitionList: partition,
	}
}

func partitionName(partition *hive_metastore.Partition) string {
	return strings.Join(partition.GetValues(), "-")
}
