// Package arena provides a typed arena allocator: a single Arena holds many objects of a single
// type in one slice.  Each object is identified by its "index", which is 8 bytes (or 4, if the
// architecture cannot hold more bytes than that in a slice).  This is the most compact way to
// hold complex objects.
//
// There only way to release the objects is to drop the entire arena.
package arena

import (
	"bytes"
	"fmt"
	"iter"
	"slices"
)

type Index struct {
	o int
}

// Arena holds Ts packed together for fast random access.  It is an efficient way to store many
// Ts.  If T is a pointer (or smaller), this does not save anything.
type Arena[T any] interface {
	// Add copies t into the arena and returns its index.  It invalidates results from all
	// previous Gets.  The returned Index is valid for the lifetime of the arena.
	Add(t T) Index
	// Get returns the T at index or nil.  The returned pointer is valid until the next time
	// the Arena is mutated (New or Add).
	Get(index Index) *T
}

const (
	defaultGrowthFactor = 0.15
	extraGrowth         = 2
)

// New returns an Arena.  This Arena is not thread-safe.
func New[T any]() Arena[T] {
	return &arena[T]{growthFactor: defaultGrowthFactor}
}

type arena[T any] struct {
	growthFactor float64
	objects      []T
}

func (a *arena[T]) Add(t T) Index {
	offset := len(a.objects)
	if len(a.objects) == cap(a.objects) {
		a.objects = slices.Grow(a.objects, int(float64(cap(a.objects))*a.growthFactor)+extraGrowth)
	}
	a.objects = append(a.objects, t)
	return Index{offset}
}

func (a *arena[T]) Get(index Index) *T {
	offset := index.o
	if offset < 0 || offset >= len(a.objects) {
		return nil
	}
	return &a.objects[offset]
}

// Map holds it values in an Arena.  If V is a pointer (or smaller), this does not save
// anything.
type Map[K comparable, V any] interface {
	Put(k K, v V) *V
	Get(k K) *V
	Len() int
}

// Optimizer can be "optimized" after many writes and before many reads, to make them more
// efficient.  Implementing this allows map keys to be compressed into an Arena.
type Optimizer interface {
	Optimize()
}

type OptimizerMap[K comparable, V any] interface {
	Map[K, V]
	Optimizer
}

// NewMap returns a Map.  This Map is not thread-safe.
func NewMap[K comparable, V any]() Map[K, V] {
	return newArenaMap[K, V]()
}

func newArenaMap[K comparable, V any]() *arenaMap[K, V] {
	return &arenaMap[K, V]{
		indices: make(map[K]Index, 0),
		arena:   New[V](),
	}
}

type arenaMap[K comparable, V any] struct {
	indices map[K]Index
	arena   Arena[V]
}

func (m *arenaMap[K, V]) Put(k K, v V) *V {
	if index, ok := m.indices[k]; ok {
		ptr := m.arena.Get(index)
		*ptr = v
		return ptr
	} else {
		index = m.arena.Add(v)
		m.indices[k] = index
		return m.arena.Get(index)
	}
}

func (m *arenaMap[K, V]) Get(k K) *V {
	if index, ok := m.indices[k]; ok {
		return m.arena.Get(index)
	} else {
		return nil
	}
}

func (m *arenaMap[K, V]) Len() int {
	return len(m.indices)
}

func (m *arenaMap[K, V]) Entries() iter.Seq2[K, *V] {
	return func(yield func(K, *V) bool) {
		for k, i := range m.indices {
			if !yield(k, m.arena.Get(i)) {
				return
			}
		}
	}
}

const KEY_SIZE_BOUND = 16

func trimKey[K ~string](key K) [KEY_SIZE_BOUND]byte {
	if len(key) > KEY_SIZE_BOUND {
		// Keys have a fixed size, and this is really a compile-time error,
		panic(fmt.Sprintf("long key %s > %d", key, KEY_SIZE_BOUND))
	}
	var ret [KEY_SIZE_BOUND]byte
	copy(ret[:], []byte(key))
	return ret
}

type entry[V any] struct {
	k [KEY_SIZE_BOUND]byte
	v V
}

// NewBoundedKeyMap returns a Map that uses string-like keys of bounded length.  Keys are
// zero-padded, so must not end in zero bytes.  This Map is not thread-safe.
//
// It to keep keys in an Arena.  The map *panics* if it encounters a longer key.
func NewBoundedKeyMap[K ~string, V any]() OptimizerMap[K, V] {
	return &boundedArenaMap[K, V]{
		bigMap:   nil,
		smallMap: newArenaMap[K, V](),
	}
}

type boundedArenaMap[K ~string, V any] struct {
	// bigMap is sorted slice of pairs.  Apart from calls to Optimize it is immutable,
	bigMap []entry[V]
	// smallMap holds values before Optimize.
	smallMap *arenaMap[K, V]
}

func (m *boundedArenaMap[K, V]) compareKey(p entry[V], k K) int {
	trimmedKey := trimKey(k)
	return bytes.Compare(p.k[:], trimmedKey[:])
}

func (m *boundedArenaMap[K, V]) compareEntries(a, b entry[V]) int {
	return bytes.Compare(a.k[:], b.k[:])
}

func (m *boundedArenaMap[K, V]) Put(k K, v V) *V {
	return m.smallMap.Put(k, v)
}

func (m *boundedArenaMap[K, V]) Get(k K) *V {
	if v := m.smallMap.Get(k); v != nil {
		return v
	}
	if m.bigMap == nil {
		return nil
	}

	pos, ok := slices.BinarySearchFunc(m.bigMap, k, m.compareKey)
	if !ok {
		return nil
	}
	return &m.bigMap[pos].v
}

func (m *boundedArenaMap[K, V]) Len() int {
	return len(m.bigMap) + m.smallMap.Len()
}

func (m *boundedArenaMap[K, V]) Optimize() {
	// Resize m.bigMap once in advance.
	m.bigMap = slices.Grow(m.bigMap, m.smallMap.Len())
	for k, v := range m.smallMap.Entries() {
		m.bigMap = append(m.bigMap, entry[V]{trimKey(k), *v})
	}
	slices.SortStableFunc(m.bigMap, m.compareEntries)

	// Deduplicate entries with the same key, keeping the last occurrence
	if len(m.bigMap) > 0 {
		writeIdx := 0
		for readIdx := 1; readIdx < len(m.bigMap); readIdx++ {
			if m.compareEntries(m.bigMap[writeIdx], m.bigMap[readIdx]) != 0 {
				writeIdx++
				m.bigMap[writeIdx] = m.bigMap[readIdx]
			} else {
				// Same key - keep the later one (at readIdx)
				m.bigMap[writeIdx] = m.bigMap[readIdx]
			}
		}
		m.bigMap = m.bigMap[:writeIdx+1]
	}

	m.smallMap = newArenaMap[K, V]()
}
