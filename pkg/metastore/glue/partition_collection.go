package glue

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/treeverse/lakefs/pkg/metastore"
)

type PartitionCollection struct {
	partitions []*glue.Partition
}

func (p *PartitionCollection) Name(i int) string {
	return partitionName(p.partitions[i])
}

func (p *PartitionCollection) Len() int {
	return len(p.partitions)
}

func (p *PartitionCollection) Less(i, j int) bool {
	return comparePartitions(p.partitions[i], p.partitions[j]) == metastore.ItemLess
}

func (p *PartitionCollection) Swap(i, j int) {
	p.partitions[i], p.partitions[j] = p.partitions[j], p.partitions[i]
}

func (p *PartitionCollection) Value(i int) interface{} {
	return p.partitions[i]
}

func (p *PartitionCollection) CompareWith(i int, v interface{}, j int) metastore.CompareResult {
	if otherIter, ok := v.(*PartitionCollection); ok {
		return comparePartitions(p.partitions[i], otherIter.partitions[j])
	}
	err := fmt.Errorf("%w *glue.PartitionCollection, got %T", ErrExpectedType, v)
	panic(err)
}

func NewPartitionCollection(partitions []*glue.Partition) *PartitionCollection {
	return &PartitionCollection{
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
	}
	if nameA > nameB {
		return metastore.ItemGreater
	}
	if partitionValueEqual(partitionA, partitionB) {
		return metastore.ItemSame
	}
	return metastore.ItemSameKey
}
