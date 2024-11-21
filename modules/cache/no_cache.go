package cache

var NoCache Cache = &noCache{}

type noCache struct{}

func (m *noCache) GetOrSet(_ interface{}, setFn SetFn) (v interface{}, err error) {
	return setFn()
}

func (m *noCache) GetOrSetWithExpiry(_ interface{}, setFn SetFnWithExpiry) (v interface{}, err error) {
	v, _, err = setFn()
	return v, err
}
