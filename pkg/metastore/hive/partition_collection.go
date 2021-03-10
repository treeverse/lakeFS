package hive

import (
	"fmt"
	"strings"

	"github.com/treeverse/lakefs/pkg/metastore"
	"github.com/treeverse/lakefs/pkg/metastore/hive/gen-go/hive_metastore"
)

type PartitionCollection struct {
	partitionList []*hive_metastore.Partition
}

func (p *PartitionCollection) Name(i int) string {
	return partitionName(p.partitionList[i])
}

func (p *PartitionCollection) Value(i int) interface{} {
	return p.partitionList[i]
}

func (p *PartitionCollection) CompareWith(i int, v interface{}, j int) metastore.CompareResult {
	if partition, ok := v.(*PartitionCollection); ok {
		return compare(p.partitionList[i], partition.partitionList[j])
	}

	err := fmt.Errorf("%w *hive.PartitionCollection got:%T", ErrExpectedType, v)
	panic(err)
}

func (p *PartitionCollection) Len() int {
	return len(p.partitionList)
}

func valueEqual(a, b *hive_metastore.Partition) bool {
	// compare only time and number of columns
	return a.LastAccessTime == b.LastAccessTime && len(a.GetSd().GetCols()) == len(b.GetSd().GetCols())
}

func compare(partitionA, partitionB *hive_metastore.Partition) metastore.CompareResult {
	if partitionName(partitionA) < partitionName(partitionB) {
		return metastore.ItemLess
	}
	if partitionName(partitionA) > partitionName(partitionB) {
		return metastore.ItemGreater
	}
	if valueEqual(partitionA, partitionB) {
		return metastore.ItemSame
	}
	return metastore.ItemSameKey
}

func (p *PartitionCollection) Less(i, j int) bool {
	return compare(p.partitionList[i], p.partitionList[j]) == metastore.ItemLess
}

func (p *PartitionCollection) Swap(i, j int) {
	p.partitionList[i], p.partitionList[j] = p.partitionList[j], p.partitionList[i]
}

func NewPartitionCollection(partition []*hive_metastore.Partition) *PartitionCollection {
	return &PartitionCollection{
		partitionList: partition,
	}
}

func partitionName(partition *hive_metastore.Partition) string {
	return strings.Join(partition.GetValues(), "-")
}
