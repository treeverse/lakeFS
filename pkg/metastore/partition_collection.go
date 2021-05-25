package metastore

import (
	"fmt"
)

type PartitionCollection struct {
	partitions []*Partition
}

func NewPartitionCollection(partitions []*Partition) *PartitionCollection {
	return &PartitionCollection{partitions: partitions}
}

func (p *PartitionCollection) Name(i int) string {
	return p.partitions[i].Name()
}

func (p *PartitionCollection) Len() int {
	return len(p.partitions)
}

func (p *PartitionCollection) Less(i, j int) bool {
	return comparePartitions(p.partitions[i], p.partitions[j]) == ItemLess
}

func (p *PartitionCollection) Swap(i, j int) {
	p.partitions[i], p.partitions[j] = p.partitions[j], p.partitions[i]
}

func (p *PartitionCollection) Value(i int) interface{} {
	return p.partitions[i]
}

func (p *PartitionCollection) CompareWith(i int, v interface{}, j int) CompareResult {
	if otherIter, ok := v.(*PartitionCollection); ok {
		return comparePartitions(p.partitions[i], otherIter.partitions[j])
	}
	err := fmt.Errorf("%w *PartitionCollection, got %T", ErrExpectedType, v)
	panic(err)
}

func partitionValueEqual(partitionA, partitionB *Partition) bool {
	if partitionA.Sd == nil && partitionB.Sd == nil {
		return true
	}
	if partitionA.Sd == nil || partitionB.Sd == nil {
		return false
	}
	return partitionA.LastAccessTime == partitionB.LastAccessTime && len(partitionA.Sd.Cols) == len(partitionB.Sd.Cols)
}

func comparePartitions(partitionA, partitionB *Partition) CompareResult {
	nameA, nameB := partitionA.Name(), partitionB.Name()
	if nameA < nameB {
		return ItemLess
	}
	if nameA > nameB {
		return ItemGreater
	}
	if partitionValueEqual(partitionA, partitionB) {
		return ItemSame
	}
	return ItemSameKey
}
