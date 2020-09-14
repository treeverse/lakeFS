package auth

import (
	"time"

	"github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/cache"
)

type CredentialSetFn func() (*model.Credential, error)
type UserSetFn func() (*model.User, error)
type UserPoliciesSetFn func() ([]*model.Policy, error)

type Cache interface {
	GetCredential(accessKeyID string, setFn CredentialSetFn) (*model.Credential, error)
	GetUser(username string, setFn UserSetFn) (*model.User, error)
	GetUserByID(userID int, setFn UserSetFn) (*model.User, error)
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

func (c *LRUCache) GetUser(username string, setFn UserSetFn) (*model.User, error) {
	v, err := c.userCache.GetOrSet(username, func() (interface{}, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v.(*model.User), nil
}

func (c *LRUCache) GetUserByID(userID int, setFn UserSetFn) (*model.User, error) {
	v, err := c.userCache.GetOrSet(userID, func() (interface{}, error) { return setFn() })
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

type DummyCache struct {
}

func (d *DummyCache) GetCredential(accessKeyID string, setFn CredentialSetFn) (*model.Credential, error) {
	return setFn()
}

func (d *DummyCache) GetUser(username string, setFn UserSetFn) (*model.User, error) {
	return setFn()
}

func (d *DummyCache) GetUserByID(userID int, setFn UserSetFn) (*model.User, error) {
	return setFn()
}

func (d *DummyCache) GetUserPolicies(userID string, setFn UserPoliciesSetFn) ([]*model.Policy, error) {
	return setFn()
}
