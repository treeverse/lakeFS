package mocks

type CacheMap struct {
	cache map[string]interface{}
}

func NewCacheMap(cacheMaxSize int) *CacheMap {
	return &CacheMap{
		cache: make(map[string]interface{}, cacheMaxSize),
	}
}

func (c *CacheMap) Get(key string) (interface{}, bool) {
	val, exist := c.cache[string(key)]
	return val, exist
}

func (c *CacheMap) Set(key string, val interface{}) {
	c.cache[key] = val
}
