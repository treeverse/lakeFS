package local_test

import (
	"container/heap"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/local"
)

func TestStringHeap(t *testing.T) {
	// Some items and their priorities.
	items := []string{"imported/0000/1", "imported../00000", "imported/00010/1", "imported./0000/1"}

	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	pq := local.StringHeap{}
	heap.Init(&pq)
	for _, item := range items {
		heap.Push(&pq, item)
	}

	// Take the items out; they arrive in decreasing priority order.
	require.Equal(t, len(items), pq.Len())
	sort.Strings(items)
	for i := range items {
		item := heap.Pop(&pq).(string)
		require.Equal(t, items[i], item)
	}
	require.Zero(t, pq.Len())
}
