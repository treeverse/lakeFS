package auth

import (
	"time"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/cache"
)

type CredentialSetFn func() (*model.Credential, error)
type UserSetFn func() (*model.User, error)
type UserPoliciesSetFn func() ([]*model.Policy, error)

type UserKey struct {
	id         string
	Username   string
	ExternalID string
	Email      string
}

type Cache interface {
	GetCredential(accessKeyID string, setFn CredentialSetFn) (*model.Credential, error)
	GetUser(key UserKey, setFn UserSetFn) (*model.User, error)
	GetUserPolicies(userID string, setFn UserPoliciesSetFn) ([]*model.Policy, error)
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

func (c *LRUCache) GetCredential(accessKeyID string, setFn CredentialSetFn) (*model.Credential, error) {
	v, err := c.credentialsCache.GetOrSet(accessKeyID, func() (interface{}, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v.(*model.Credential), nil
}

func (c *LRUCache) GetUser(key UserKey, setFn UserSetFn) (*model.User, error) {
	v, err := c.userCache.GetOrSet(key, func() (interface{}, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v.(*model.User), nil
}

func (c *LRUCache) GetUserPolicies(userID string, setFn UserPoliciesSetFn) ([]*model.Policy, error) {
	v, err := c.policyCache.GetOrSet(userID, func() (interface{}, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v.([]*model.Policy), nil
}

// DummyCache dummy cache that doesn't cache
type DummyCache struct{}

func (d *DummyCache) GetCredential(_ string, setFn CredentialSetFn) (*model.Credential, error) {
	return setFn()
}

func (d *DummyCache) GetUser(_ UserKey, setFn UserSetFn) (*model.User, error) {
	return setFn()
}

func (d *DummyCache) GetUserPolicies(_ string, setFn UserPoliciesSetFn) ([]*model.Policy, error) {
	return setFn()
}
