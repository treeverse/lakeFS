package local_test

import (
	"container/heap"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/local"
)

func TestStringHeap(t *testing.T) {
	// Some items and their priorities.
	items := []string{"imported/0000/1", "imported../00000", "imported/00010/1"}

	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	pq := local.StringHeap{}
	heap.Init(&pq)

	for _, item := range items {
		heap.Push(&pq, item)
	}

	// Insert a new item and then modify its priority.
	items = append(items, "imported./0000/1")
	heap.Push(&pq, "imported./0000/1")

	// Take the items out; they arrive in decreasing priority order.
	sort.Strings(items)
	fmt.Println(items)
	i := 0
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(string)
		require.Equal(t, items[i], item)
		i++
	}
}
