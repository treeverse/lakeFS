package catalog

type resultRowHeapType struct {
	heap              []resultRow
	branchPriorityMap map[int64]int
}

func (h resultRowHeapType) Len() int { return len(h.heap) }
func (h resultRowHeapType) Less(i, j int) bool {
	if (h.heap[i]).PathSuffix == h.heap[j].PathSuffix {
		iBranchID := h.heap[i].BranchID
		jBranchID := h.heap[j].BranchID
		return h.branchPriorityMap[iBranchID] < h.branchPriorityMap[jBranchID]
	} else {
		return h.heap[i].PathSuffix < h.heap[j].PathSuffix
	}
}
func (h resultRowHeapType) Swap(i, j int) { h.heap[i], h.heap[j] = h.heap[j], h.heap[i] }
func (h *resultRowHeapType) Push(x interface{}) {
	h.heap = append(h.heap, x.(resultRow))
}
func (h *resultRowHeapType) Pop() interface{} {
	old := (*h).heap
	n := len(old)
	x := old[n-1]
	h.heap = old[0 : n-1]
	return x
}
