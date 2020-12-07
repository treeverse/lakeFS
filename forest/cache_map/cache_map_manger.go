package cachemap

import (
	"container/heap"
)

type cacheEntry struct {
	key     string
	weight  int64
	content *interface{}
}

type cacheMap struct {
	cacheMaxSize     int
	cacheTrimSize    int
	initialWeight    int64
	additionalWeight int64
	opsNumber        int
	trimFactor       int
	cache            map[string]*cacheEntry
}

func newCacheMap(cacheMaxSize, cacheTrimSize int, initialWeight, additionalWeight int64, trimFactor int, OpsBeforeTrim int) cacheMap {
	return cacheMap{
		cacheMaxSize:     cacheMaxSize,
		cacheTrimSize:    cacheTrimSize,
		initialWeight:    initialWeight,
		additionalWeight: additionalWeight,
		trimFactor:       trimFactor,
		cache:            make(map[string]*cacheEntry),
	}
}

func (c *cacheMap) Get(k string) (*interface{}, bool) {
	t, ok := c.cache[k]
	if ok {
		t.weight += c.additionalWeight
		c.decayWeightWhenNeeded()
		return t.content, true
	}
	return nil, false
}

func (c *cacheMap) decayWeightWhenNeeded() {
	c.opsNumber++
	if c.opsNumber > c.cacheMaxSize*c.trimFactor {
		for _, v := range c.cache {
			v.weight /= 2
		}
		c.opsNumber = 0
	}
}

func (c *cacheMap) Set(k string, val *interface{}) {
	c.cache[k] = &cacheEntry{
		key:     k,
		weight:  c.initialWeight,
		content: val,
	}
	if len(c.cache) > c.cacheMaxSize {
		c.trimCache()
	}
}

// implementing partial sort using hash
type heapItem struct {
	key    string
	weight int64
}
type heapItemsType []heapItem

func (c *cacheMap) trimCache() {
	limit := c.cacheTrimSize
	heapItems := make(heapItemsType, limit)
	var counter int
	for k, v := range c.cache {
		if counter < limit {
			heapItems[counter] = heapItem{
				key:    k,
				weight: v.weight}
		}
		if counter == limit {
			heap.Init(&heapItems)
		}
		if counter >= limit {
			if heapItems[0].weight > v.weight {
				heap.Pop(&heapItems)
				heap.Push(&heapItems, heapItem{
					key:    k,
					weight: v.weight})
			}
		}
		counter++
	}
	for _, minItem := range heapItems {
		delete(c.cache, minItem.key)
	}
}

func (h heapItemsType) Len() int           { return len(h) }
func (h heapItemsType) Less(i, j int) bool { return h[i].weight > h[j].weight }
func (h heapItemsType) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *heapItemsType) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
func (h *heapItemsType) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}
