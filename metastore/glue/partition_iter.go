package glue

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/treeverse/lakefs/metastore"
)

type PartitionIter struct {
	partitions []*glue.Partition
}

func (p *PartitionIter) Name(i int) string {
	return partitionName(p.partitions[i])
}

func (p *PartitionIter) Len() int {
	return len(p.partitions)
}

func (p *PartitionIter) Less(i, j int) bool {
	return comparePartitions(p.partitions[i], p.partitions[j]) == metastore.ItemLess
}

func (p *PartitionIter) Swap(i, j int) {
	p.partitions[i], p.partitions[j] = p.partitions[j], p.partitions[i]
}

func (p *PartitionIter) Value(i int) interface{} {
	return p.partitions[i]
}

func (p *PartitionIter) CompareWith(i int, v interface{}, j int) metastore.CompareResult {
	if otherIter, ok := v.(*PartitionIter); ok {
		return comparePartitions(p.partitions[i], otherIter.partitions[j])
	}
	msg := fmt.Sprintf("expected type *glue.PartitionIter, got %T.", v)
	panic(msg)
}

func NewPartitionIter(partitions []*glue.Partition) *PartitionIter {
	return &PartitionIter{
		partitions: partitions,
	}
}

func partitionName(partition *glue.Partition) string {
	return strings.Join(aws.StringValueSlice(partition.Values), "-")
}

func partitionColumnAmount(partition *glue.Partition) int {
	if partition.StorageDescriptor == nil || partition.StorageDescriptor.Columns == nil {
		return 0
	}
	return len(partition.StorageDescriptor.Columns)
}

func partitionValueEqual(partitionA, partitionB *glue.Partition) bool {
	noAccessTime := partitionA.LastAccessTime == nil && partitionB.LastAccessTime == nil
	timeEqual := (partitionA.LastAccessTime != nil && aws.TimeValue(partitionA.LastAccessTime).Equal(aws.TimeValue(partitionB.LastAccessTime))) || noAccessTime
	return timeEqual && partitionColumnAmount(partitionA) == partitionColumnAmount(partitionB)
}

func comparePartitions(partitionA, partitionB *glue.Partition) metastore.CompareResult {
	nameA, nameB := partitionName(partitionA), partitionName(partitionB)
	if nameA < nameB {
		return metastore.ItemLess
	} else if nameA == nameB {
		if partitionValueEqual(partitionA, partitionB) {
			return metastore.ItemSame
		}
		return metastore.ItemSameKey
	}
	return metastore.ItemGreater
}
