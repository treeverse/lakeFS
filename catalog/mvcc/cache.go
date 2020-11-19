package mvcc

import (
	"time"

	"github.com/treeverse/lakefs/cache"
	"github.com/treeverse/lakefs/catalog"
)

type GetRepositoryFn func(repository string) (*catalog.Repository, error)
type GetRepositoryIDFn func(repository string) (int, error)
type GetBranchIDFn func(repository string, branch string) (int64, error)

type Cache interface {
	Repository(repository string, setFn GetRepositoryFn) (*catalog.Repository, error)
	RepositoryID(repository string, setFn GetRepositoryIDFn) (int, error)
	BranchID(repository string, branch string, setFn GetBranchIDFn) (int64, error)
}

type LRUCache struct {
	repository   cache.Cache
	repositoryID cache.Cache
	branchID     cache.Cache
}

func NewLRUCache(size int, expiry, jitter time.Duration) *LRUCache {
	jitterFn := cache.NewJitterFn(jitter)
	return &LRUCache{
		repository:   cache.NewCache(size, expiry, jitterFn),
		repositoryID: cache.NewCache(size, expiry, jitterFn),
		branchID:     cache.NewCache(size, expiry, jitterFn),
	}
}

func (c *LRUCache) Repository(repository string, setFn GetRepositoryFn) (*catalog.Repository, error) {
	v, err := c.repository.GetOrSet(repository, func() (interface{}, error) {
		return setFn(repository)
	})
	if err != nil {
		return nil, err
	}
	return v.(*catalog.Repository), nil
}

func (c *LRUCache) RepositoryID(repository string, setFn GetRepositoryIDFn) (int, error) {
	v, err := c.repositoryID.GetOrSet(repository, func() (interface{}, error) {
		return setFn(repository)
	})
	if err != nil {
		return 0, err
	}
	return v.(int), nil
}

func (c *LRUCache) BranchID(repository string, branch string, setFn GetBranchIDFn) (int64, error) {
	key := repository + "/" + branch
	v, err := c.branchID.GetOrSet(key, func() (interface{}, error) {
		return setFn(repository, branch)
	})
	if err != nil {
		return 0, err
	}
	return v.(int64), nil
}

type DummyCache struct{}

func (c *DummyCache) Repository(repository string, setFn GetRepositoryFn) (*catalog.Repository, error) {
	return setFn(repository)
}

func (c *DummyCache) RepositoryID(repository string, setFn GetRepositoryIDFn) (int, error) {
	return setFn(repository)
}

func (c *DummyCache) BranchID(repository string, branch string, setFn GetBranchIDFn) (int64, error) {
	return setFn(repository, branch)
}
