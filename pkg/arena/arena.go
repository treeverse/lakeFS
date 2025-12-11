// Package arena provides a typed arena allocator: a single Arena holds many objects of a single
// type in one slice.  Each object is identified by its "index", which is 8 bytes (or 4, if the
// architecture cannot hold more bytes than that in a slice).  This is the most compact way to
// hold complex objects.
//
// There only way to release the objects is to drop the entire arena.
package arena

import "slices"

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
	defaultGrowthFactor = 1.15
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
}

// NewMap returns a Map.  This Map is not thread-safe.
func NewMap[K comparable, V any]() Map[K, V] {
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

// NewBoundedKeyMap returns a Map that uses string-like keys of bounded length.  This allows it
// to keep keys in an Arena.  The map *panics* if it encounters a longer key.
type boundedArenaMap[K ~string, V any, SIZE int] struct {
}
