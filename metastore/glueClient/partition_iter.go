package glueClient

import (
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/treeverse/lakefs/metastore"
)

type PartitionIter struct {
	partitions []*glue.Partition
	index      int
}

func NewPartitionIter(partitions []*glue.Partition) *PartitionIter {
	return &PartitionIter{
		partitions: partitions,
		index:      0,
	}
}
func (p *PartitionIter) getCurrent() *glue.Partition {
	return p.partitions[p.index]
}

func partitionName(partition *glue.Partition) string {
	return strings.Join(aws.StringValueSlice(partition.Values), "-")
}

func comparePartitions(partitionA, partitionB *glue.Partition) bool {
	return partitionName(partitionA) <= partitionName(partitionB)
}

func (p *PartitionIter) Sort() {
	partitions := p.partitions
	sort.Slice(partitions, func(i, j int) bool {
		return comparePartitions(partitions[i], partitions[j])
	})
}

func (p *PartitionIter) HasMore() bool {
	return len(p.partitions) > p.index
}

func (p *PartitionIter) Next() {
	p.index++
}

func (p *PartitionIter) GetName() string {
	return partitionName(p.getCurrent())
}

func (p *PartitionIter) GreaterEqual(other metastore.ComparableIterator) bool {
	return p.GetName() <= other.GetName()
}

func (p *PartitionIter) KeyEqual(other metastore.ComparableIterator) bool {
	return p.GetName() == other.GetName()
}

func partitionColumnAmount(partition *glue.Partition) int {
	if partition.StorageDescriptor == nil || partition.StorageDescriptor.Columns == nil {
		return 0
	}
	return len(partition.StorageDescriptor.Columns)
}

func (p *PartitionIter) ValueEqual(other metastore.ComparableIterator) bool {
	// compare last timestamp and #columns
	partition := p.getCurrent()
	otherPartition := other.(*PartitionIter).getCurrent()
	noAccessTime := partition.LastAccessTime == nil && otherPartition.LastAccessTime == nil
	timeEqual := (partition.LastAccessTime != nil && aws.TimeValue(partition.LastAccessTime).Equal(aws.TimeValue(otherPartition.LastAccessTime))) || noAccessTime
	return timeEqual && partitionColumnAmount(partition) == partitionColumnAmount(otherPartition)
}
