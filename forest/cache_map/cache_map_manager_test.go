package cachemap

import (
	"fmt"
	"math/rand"
	"testing"
)

type cacheTest struct {
	name             string
	cacheMaxSize     int
	cacheTrimSize    int
	initialWeight    int64
	additionalWeight int64
	trimFactor       int
	randomOpsFactor  int
}

var tests = []cacheTest{
	{"initialTest", 100, 20, 64, 16, 1, 10},
	{"bigTest", 1000, 200, 64, 16, 2, 20},
	{"largeTrim", 500, 300, 64, 16, 3, 30},
	{"smallWeight", 500, 300, 8, 2, 4, 40},
	{"minimalTrim", 500, 30, 64, 4, 4, 40},
}

func TestTrim(t *testing.T) {
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			k := newCacheMap(test.cacheMaxSize, test.cacheTrimSize, test.initialWeight, test.additionalWeight, test.trimFactor, test.trimFactor)
			for i := 0; i < k.cacheMaxSize; i++ {
				key := fmt.Sprintf("%05d", i)
				k.Set(key, nil)
			}
			for i := 0; i < k.cacheMaxSize*test.randomOpsFactor; i++ {
				key := fmt.Sprintf("%05d", rand.Int31n(int32(k.cacheMaxSize)))
				k.Get(key)
			}
			cacheCopy := deepCopyMap(k.cache)
			k.Set("spillValue", nil)
			maxRemoved := maxRemoved(k.cache, cacheCopy)
			for _, v := range k.cache {
				if v.weight < maxRemoved {
					t.Errorf("eviction erroe, max removed: %d, weight of not removed:%d", maxRemoved, v.weight)
				}
			}
		})

	}
}

func deepCopyMap(inp map[string]*cacheEntry) map[string]*cacheEntry {
	out := make(map[string]*cacheEntry, len(inp))
	for k, v := range inp {
		out[k] = v
	}
	return out
}
func maxRemoved(a, b map[string]*cacheEntry) int64 {
	var maxRemoved int64
	for k, v := range b {
		_, exist := a[k]
		if !exist {
			w := v.weight
			if w > maxRemoved {
				maxRemoved = w
			}
		}
	}
	return maxRemoved
}
