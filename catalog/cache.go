package catalog

import (
	"time"

	"github.com/treeverse/lakefs/cache"
)

type GetRepositoryIDFn func(repository string) (int, error)
type GetBranchIDFn func(repository string, branch string) (int64, error)

type Cache interface {
	RepositoryID(repository string, setFn GetRepositoryIDFn) (int, error)
	BranchID(repository string, branch string, setFn GetBranchIDFn) (int64, error)
}

type LRUCache struct {
	repositoryCache cache.Cache
	branchCache     cache.Cache
}

func NewLRUCache(size int, expiry, jitter time.Duration) *LRUCache {
	jitterFn := cache.NewJitterFn(jitter)
	return &LRUCache{
		repositoryCache: cache.NewCache(size, expiry, jitterFn),
		branchCache:     cache.NewCache(size, expiry, jitterFn),
	}
}

func (c *LRUCache) RepositoryID(repository string, setFn GetRepositoryIDFn) (int, error) {
	v, err := c.repositoryCache.GetOrSet(repository, func() (interface{}, error) {
		return setFn(repository)
	})
	if err != nil {
		return 0, err
	}
	return v.(int), nil
}

func (c *LRUCache) BranchID(repository string, branch string, setFn GetBranchIDFn) (int64, error) {
	key := repository + "/" + branch
	v, err := c.branchCache.GetOrSet(key, func() (interface{}, error) {
		return setFn(repository, branch)
	})
	if err != nil {
		return 0, err
	}
	return v.(int64), nil
}

type DummyCache struct{}

func (c *DummyCache) RepositoryID(repository string, setFn GetRepositoryIDFn) (int, error) {
	return setFn(repository)
}

func (c *DummyCache) BranchID(repository string, branch string, setFn GetBranchIDFn) (int64, error) {
	return setFn(repository, branch)
}
