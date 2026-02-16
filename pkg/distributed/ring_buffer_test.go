package distributed_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/distributed"
)

func TestRingBuffer_PushPop(t *testing.T) {
	rb := distributed.NewRingBuffer[int](0) // tests default cap branch
	for i := range 10 {
		rb.Push(i)
	}
	if rb.Len() != 10 {
		t.Fatalf("expected len 10, got %d", rb.Len())
	}
	for i := range 10 {
		got := rb.Pop()
		if got != i {
			t.Fatalf("Pop %d: expected %d, got %d", i, i, got)
		}
	}
	if rb.Len() != 0 {
		t.Fatalf("expected empty, got len %d", rb.Len())
	}
}

func TestRingBuffer_Peek(t *testing.T) {
	rb := distributed.NewRingBuffer[string](4)
	rb.Push("a")
	rb.Push("b")
	if got := rb.Peek(); got != "a" {
		t.Fatalf("Peek: expected 'a', got %q", got)
	}
	// Peek should not remove the element.
	if rb.Len() != 2 {
		t.Fatalf("Peek changed len: got %d", rb.Len())
	}
}

func TestRingBuffer_PeekEmpty(t *testing.T) {
	rb := distributed.NewRingBuffer[int](4)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on Peek of empty buffer")
		}
	}()
	rb.Peek()
}

func TestRingBuffer_PopEmpty(t *testing.T) {
	rb := distributed.NewRingBuffer[int](4)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on Pop of empty buffer")
		}
	}()
	rb.Pop()
}

func TestRingBuffer_Remove(t *testing.T) {
	rb := distributed.NewRingBuffer[int](4)
	rb.Push(1)
	rb.Push(2)
	rb.Push(3)

	// Remove middle element.
	found := rb.Remove(func(v int) bool { return v == 2 })
	if !found {
		t.Fatal("Remove returned false for existing element")
	}
	if rb.Len() != 2 {
		t.Fatalf("expected len 2, got %d", rb.Len())
	}
	if got := rb.Pop(); got != 1 {
		t.Fatalf("expected 1, got %d", got)
	}
	if got := rb.Pop(); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}

func TestRingBuffer_RemoveNotFound(t *testing.T) {
	rb := distributed.NewRingBuffer[int](4)
	rb.Push(1)
	found := rb.Remove(func(v int) bool { return v == 99 })
	if found {
		t.Fatal("Remove returned true for missing element")
	}
	if rb.Len() != 1 {
		t.Fatalf("expected len 1, got %d", rb.Len())
	}
}

func TestRingBuffer_GrowAndShrink(t *testing.T) {
	rb := distributed.NewRingBuffer[int](2)
	// Push beyond initial capacity to trigger grow.
	for i := range 20 {
		rb.Push(i)
	}
	if rb.Len() != 20 {
		t.Fatalf("expected len 20, got %d", rb.Len())
	}
	// Pop most elements to trigger shrink.
	for range 18 {
		rb.Pop()
	}
	if rb.Len() != 2 {
		t.Fatalf("expected len 2, got %d", rb.Len())
	}
	// Remaining elements should be correct.
	if got := rb.Pop(); got != 18 {
		t.Fatalf("expected 18, got %d", got)
	}
	if got := rb.Pop(); got != 19 {
		t.Fatalf("expected 19, got %d", got)
	}
}

func TestRingBuffer_Wraparound(t *testing.T) {
	rb := distributed.NewRingBuffer[int](4)
	// Push and pop to advance head past the start of the buffer.
	for i := range 3 {
		rb.Push(i)
	}
	for range 3 {
		rb.Pop()
	}
	// Now head is at index 3. Push enough to wrap around.
	for i := range 5 {
		rb.Push(i + 10)
	}
	for i := range 5 {
		got := rb.Pop()
		if got != i+10 {
			t.Fatalf("expected %d, got %d", i+10, got)
		}
	}
}

func TestRingBuffer_RemoveWraparound(t *testing.T) {
	rb := distributed.NewRingBuffer[int](4)
	// Advance head to create wraparound.
	for i := range 3 {
		rb.Push(i)
	}
	for range 3 {
		rb.Pop()
	}
	// Push elements that wrap around the backing array.
	rb.Push(10)
	rb.Push(20)
	rb.Push(30)
	rb.Push(40)

	// Remove an element in the wrapped portion.
	rb.Remove(func(v int) bool { return v == 20 })

	expected := []int{10, 30, 40}
	for _, want := range expected {
		got := rb.Pop()
		if got != want {
			t.Fatalf("expected %d, got %d", want, got)
		}
	}
}
