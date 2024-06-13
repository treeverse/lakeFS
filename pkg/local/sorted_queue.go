package local

// A SortedQueue implements heap.Interface and holds strings.
type SortedQueue []string

func (pq *SortedQueue) Len() int { return len(*pq) }

func (pq *SortedQueue) Less(i, j int) bool {
	// We want Pop to give us the smallest
	return (*pq)[i] < (*pq)[j]
}

func (pq *SortedQueue) Swap(i, j int) {
	(*pq)[i], (*pq)[j] = (*pq)[j], (*pq)[i]
}

func (pq *SortedQueue) Push(x any) {
	item := x.(string)
	*pq = append(*pq, item)
}

func (pq *SortedQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
