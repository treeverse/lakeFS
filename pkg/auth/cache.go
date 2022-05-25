package auth

import (
	"time"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/cache"
)

type CredentialSetFn func() (*model.KvCredential, error)
type UserSetFn func() (*model.KvUser, error)
type UserPoliciesSetFn func() ([]*model.KvPolicy, error)

type Cache interface {
	GetCredential(accessKeyID string, setFn CredentialSetFn) (*model.KvCredential, error)
	GetUser(username string, setFn UserSetFn) (*model.KvUser, error)
	GetUserByID(userID int64, setFn UserSetFn) (*model.KvUser, error)
	GetUserPolicies(userID string, setFn UserPoliciesSetFn) ([]*model.KvPolicy, error)
}

type LRUCache struct {
	credentialsCache cache.Cache
	userCache        cache.Cache
	policyCache      cache.Cache
}

func NewLRUCache(size int, expiry, jitter time.Duration) *LRUCache {
	jitterFn := cache.NewJitterFn(jitter)
	return &LRUCache{
		credentialsCache: cache.NewCache(size, expiry, jitterFn),
		userCache:        cache.NewCache(size, expiry, jitterFn),
		policyCache:      cache.NewCache(size, expiry, jitterFn),
	}
}

func (c *LRUCache) GetCredential(accessKeyID string, setFn CredentialSetFn) (*model.KvCredential, error) {
	v, err := c.credentialsCache.GetOrSet(accessKeyID, func() (interface{}, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v.(*model.KvCredential), nil
}

func (c *LRUCache) GetUser(username string, setFn UserSetFn) (*model.KvUser, error) {
	v, err := c.userCache.GetOrSet(username, func() (interface{}, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v.(*model.KvUser), nil
}

func (c *LRUCache) GetUserByID(userID int64, setFn UserSetFn) (*model.KvUser, error) {
	v, err := c.userCache.GetOrSet(userID, func() (interface{}, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v.(*model.KvUser), nil
}

func (c *LRUCache) GetUserPolicies(userID string, setFn UserPoliciesSetFn) ([]*model.KvPolicy, error) {
	v, err := c.policyCache.GetOrSet(userID, func() (interface{}, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v.([]*model.KvPolicy), nil
}

// DummyCache dummy cache that doesn't cache
type DummyCache struct{}

func (d *DummyCache) GetCredential(_ string, setFn CredentialSetFn) (*model.KvCredential, error) {
	return setFn()
}

func (d *DummyCache) GetUser(_ string, setFn UserSetFn) (*model.KvUser, error) {
	return setFn()
}

func (d *DummyCache) GetUserByID(_ int64, setFn UserSetFn) (*model.KvUser, error) {
	return setFn()
}

func (d *DummyCache) GetUserByEmail(_ string, setFn UserSetFn) (*model.KvUser, error) {
	return setFn()
}

func (d *DummyCache) GetUserPolicies(_ string, setFn UserPoliciesSetFn) ([]*model.KvPolicy, error) {
	return setFn()
}
