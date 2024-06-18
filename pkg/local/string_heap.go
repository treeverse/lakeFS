package local

// A StringHeap is a min-heap of strings
type StringHeap []string

func (pq *StringHeap) Len() int { return len(*pq) }

func (pq *StringHeap) Less(i, j int) bool {
	// We want Pop to give us the smallest string
	return (*pq)[i] < (*pq)[j]
}

func (pq *StringHeap) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *StringHeap) Push(x any) {
	*pq = append(*pq, x.(string))
}

func (pq *StringHeap) Pop() any {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

// Peek - according to heap documentation (https://pkg.go.dev/container/heap) "The minimum element in the tree is the root, at index 0."
func (pq *StringHeap) Peek() any {
	x := (*pq)[0]
	return x
}
