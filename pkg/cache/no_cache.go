package cache

var NoCache Cache = &noCache{}

type noCache struct{}

func (m *noCache) GetOrSet(_ interface{}, setFn SetFn) (v interface{}, err error) {
	return setFn()
}

func (m *noCache) Set(_, _ interface{}) {}

func (m *noCache) Clear(_ interface{}) {}
