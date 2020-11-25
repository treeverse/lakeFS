package tree

//"container/heap"

type heapNode struct {
	weight int
	key    string
}
type heapType struct {
	heap []heapNode
	size int
}

func newHeap(numberToEvict int) *heapType {
	h := make([]heapNode, numberToEvict)
	return &heapType{
		heap: h}

}

func (h *heapType) addNode(weight int, key string) {
	n := heapNode{weight: weight,
		key: key}
	if len(h.heap) < h.size {

	}
}

func (h *heapType) swap(first, second int) {
	t := h.heap[first]
	h.heap[first] = h.heap[second]
	h.heap[second] = t
}
