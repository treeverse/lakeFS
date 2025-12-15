package auth

import (
	"time"

	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/cache"
)

type (
	CredentialSetFn     func() (*model.Credential, error)
	UserSetFn           func() (*model.User, error)
	UserPoliciesSetFn   func() ([]*model.Policy, error)
	ExternalPrincipalFn func() (*model.ExternalPrincipal, error)
)

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
	GetExternalPrincipal(key string, setFn ExternalPrincipalFn) (*model.ExternalPrincipal, error)
}

type LRUCache struct {
	credentialsCache       cache.Cache[string, *model.Credential]
	userCache              cache.Cache[UserKey, *model.User]
	policyCache            cache.Cache[string, []*model.Policy]
	externalPrincipalCache cache.Cache[string, *model.ExternalPrincipal]
}

func NewLRUCache(size int, expiry time.Duration) *LRUCache {
	return &LRUCache{
		credentialsCache:       cache.NewCache[string, *model.Credential](size, expiry),
		userCache:              cache.NewCache[UserKey, *model.User](size, expiry),
		policyCache:            cache.NewCache[string, []*model.Policy](size, expiry),
		externalPrincipalCache: cache.NewCache[string, *model.ExternalPrincipal](size, expiry),
	}
}

func (c *LRUCache) GetCredential(accessKeyID string, setFn CredentialSetFn) (*model.Credential, error) {
	v, err := c.credentialsCache.GetOrSet(accessKeyID, func() (*model.Credential, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (c *LRUCache) GetUser(key UserKey, setFn UserSetFn) (*model.User, error) {
	v, err := c.userCache.GetOrSet(key, func() (*model.User, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (c *LRUCache) GetUserPolicies(userID string, setFn UserPoliciesSetFn) ([]*model.Policy, error) {
	v, err := c.policyCache.GetOrSet(userID, func() ([]*model.Policy, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (c *LRUCache) GetExternalPrincipal(key string, setFn ExternalPrincipalFn) (*model.ExternalPrincipal, error) {
	v, err := c.externalPrincipalCache.GetOrSet(key, func() (*model.ExternalPrincipal, error) { return setFn() })
	if err != nil {
		return nil, err
	}
	return v, nil
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

func (d *DummyCache) GetExternalPrincipal(_ string, setFn ExternalPrincipalFn) (*model.ExternalPrincipal, error) {
	return setFn()
}
