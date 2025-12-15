package cache

type NoCache[K comparable, V any] struct{}

func (m *NoCache[K, V]) GetOrSet(_ K, setFn SetFn[V]) (v V, err error) {
	return setFn()
}
