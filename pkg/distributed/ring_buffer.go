package distributed

// RingBuffer is a dynamically-sized FIFO queue backed by a circular buffer.
// It grows when full and shrinks when significantly underutilized.
type RingBuffer[T any] struct {
	buf  []T
	head int
	tail int
	len  int
}

const (
	// ringBufferInitialCap is the default initial capacity if none is specified.
	ringBufferInitialCap   = 4
	ringBufferGrowthFactor = 2
	// ringBufferMinShrinkCap prevents shrinking very small buffers,
	// avoiding excessive reallocations when only a few elements remain.
	ringBufferMinShrinkCap = 4
	// ringBufferShrinkFactor must be > ringBufferGrowthFactor to provide
	// hysteresis and avoid grow/shrink thrashing near the boundary.
	ringBufferShrinkFactor = 4
)

// NewRingBuffer creates a ring buffer with the given initial capacity.
// If initialCap is 0, defaults to ringBufferInitialCap.
func NewRingBuffer[T any](initialCap int) *RingBuffer[T] {
	if initialCap < 1 {
		initialCap = ringBufferInitialCap
	}
	return &RingBuffer[T]{
		buf: make([]T, initialCap),
	}
}

// Len returns the number of elements in the buffer.
func (r *RingBuffer[T]) Len() int {
	return r.len
}

// Push adds an element to the back of the queue.
func (r *RingBuffer[T]) Push(v T) {
	if r.len == len(r.buf) {
		r.grow()
	}
	r.buf[r.tail] = v
	r.tail = (r.tail + 1) % len(r.buf)
	r.len++
}

// Pop removes and returns the front element.
// Panics if the buffer is empty.
func (r *RingBuffer[T]) Pop() T {
	if r.len == 0 {
		panic("RingBuffer: Pop on empty buffer")
	}
	v := r.buf[r.head]
	var zero T
	r.buf[r.head] = zero // allow GC
	r.head++
	if r.head == len(r.buf) {
		r.head = 0
	}
	r.len--
	r.maybeShrink()
	return v
}

// Peek returns the front element without removing it.
// Panics if the buffer is empty.
func (r *RingBuffer[T]) Peek() T {
	if r.len == 0 {
		panic("RingBuffer: Peek on empty buffer")
	}
	return r.buf[r.head]
}

// Remove removes the first occurrence of an element matching the predicate.
// Returns true if an element was removed.
func (r *RingBuffer[T]) Remove(match func(T) bool) bool {
	for i := 0; i < r.len; i++ {
		idx := (r.head + i) % len(r.buf)
		if match(r.buf[idx]) {
			r.removeAt(i)
			return true
		}
	}
	return false
}

// removeAt removes the element at logical index i (0 = head).
func (r *RingBuffer[T]) removeAt(i int) {
	// Shift elements after i toward head to fill the gap.
	for j := i; j < r.len-1; j++ {
		cur := (r.head + j) % len(r.buf)
		next := (r.head + j + 1) % len(r.buf)
		r.buf[cur] = r.buf[next]
	}
	// Clear the last slot.
	var zero T
	last := (r.head + r.len - 1) % len(r.buf)
	r.buf[last] = zero
	r.tail = last
	r.len--
	r.maybeShrink()
}

// grow doubles the buffer capacity.
func (r *RingBuffer[T]) grow() {
	r.resize(len(r.buf) * ringBufferGrowthFactor)
}

// maybeShrink halves capacity if len <= cap/shrinkFactor and cap > minShrinkCap.
func (r *RingBuffer[T]) maybeShrink() {
	if len(r.buf) > ringBufferMinShrinkCap && r.len <= len(r.buf)/ringBufferShrinkFactor {
		r.resize(len(r.buf) / ringBufferGrowthFactor)
	}
}

// resize copies elements to a new buffer of the given capacity.
func (r *RingBuffer[T]) resize(newCap int) {
	newBuf := make([]T, newCap)
	n := copy(newBuf, r.buf[r.head:])
	if n < r.len {
		copy(newBuf[n:], r.buf[:r.tail])
	}
	r.buf = newBuf
	r.head = 0
	r.tail = r.len
}
