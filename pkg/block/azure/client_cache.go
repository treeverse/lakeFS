package azure

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	lru "github.com/hnlq715/golang-lru"
	"github.com/puzpuzpuz/xsync"
	"github.com/treeverse/lakefs/pkg/block/params"
	"golang.org/x/exp/slices"
)

const UDCCacheExpiry = time.Hour
const UDCCacheWorkaroundDivider = 2

type ClientCache struct {
	serviceToClient   *xsync.MapOf[string, *service.Client]
	containerToClient *xsync.MapOf[string, *container.Client]
	// udcCache - User Delegation Credential cache used to reduce POST requests while creating pre-signed URLs
	udcCache *lru.ARCCache
	params   params.Azure
}

func NewCache(p params.Azure) (*ClientCache, error) {
	l, err := lru.NewARCWithExpire(udcCacheSize, UDCCacheExpiry/UDCCacheWorkaroundDivider)
	// TODO(Guys): dividing the udc cache expiry by 2 is a workaround for the fact that this package does not handle expiry correctly, we can remove this once we use https://github.com/hashicorp/golang-lru expirables
	if err != nil {
		return nil, err
	}

	return &ClientCache{
		serviceToClient:   xsync.NewMapOf[*service.Client](),
		containerToClient: xsync.NewMapOf[*container.Client](),
		udcCache:          l,
		params:            p,
	}, nil
}

func mapKey(storageAccount, containerName string) string {
	return fmt.Sprintf("%s#%s", storageAccount, containerName)
}

func (c *ClientCache) NewContainerClient(storageAccount, containerName string) (*container.Client, error) {
	key := mapKey(storageAccount, containerName)

	var err error
	cl, _ := c.containerToClient.LoadOrCompute(key, func() *container.Client {
		var svc *service.Client
		svc, err = c.NewServiceClient(storageAccount)
		if err != nil {
			return nil
		}
		return svc.NewContainerClient(containerName)
	})
	if err != nil {
		return nil, err
	}

	return cl, nil
}

func (c *ClientCache) NewServiceClient(storageAccount string) (*service.Client, error) {
	p := c.params
	// Use StorageAccessKey to initialize storage account client only if it was provided for this given storage account
	// Otherwise fall back to the default credentials
	if p.StorageAccount != storageAccount {
		p.StorageAccount = storageAccount
		p.StorageAccessKey = ""
	}

	var err error
	cl, _ := c.serviceToClient.LoadOrCompute(storageAccount, func() *service.Client {
		var svc *service.Client
		svc, err = BuildAzureServiceClient(p)
		if err != nil {
			return nil
		}
		return svc
	})
	if err != nil {
		return nil, err
	}

	return cl, nil
}

func (c *ClientCache) NewUDC(ctx context.Context, storageAccount string, expiry *time.Time) (*service.UserDelegationCredential, error) {
	var udc *service.UserDelegationCredential
	// Check udcCache
	res, ok := c.udcCache.Get(storageAccount)
	if !ok {
		baseTime := time.Now().UTC().Add(-10 * time.Second)
		// UDC expiry time of PreSignedExpiry + hour
		udcExpiry := expiry.Add(UDCCacheExpiry)
		info := service.KeyInfo{
			Start:  to.Ptr(baseTime.UTC().Format(sas.TimeFormat)),
			Expiry: to.Ptr(udcExpiry.Format(sas.TimeFormat)),
		}
		svc, err := c.NewServiceClient(storageAccount)
		if err != nil {
			return nil, err
		}
		udc, err = svc.GetUserDelegationCredential(ctx, info, nil)
		if err != nil {
			return nil, err
		}
		// UDC expires after PreSignedExpiry + hour but cache entry expires after an hour
		c.udcCache.Add(storageAccount, udc)
	} else {
		udc = res.(*service.UserDelegationCredential)
	}
	return udc, nil
}

func BuildAzureServiceClient(params params.Azure) (*service.Client, error) {
	var endpoint string
	if params.Domain == "" {
		params.Domain = BlobEndpointDefaultDomain
	} else if !slices.Contains(supportedDomains, params.Domain) {
		return nil, ErrInvalidDomain
	}

	if params.TestEndpointURL != "" { // For testing purposes - override default endpoint template
		endpoint = params.TestEndpointURL
	} else {
		endpoint = buildAccountEndpoint(params.StorageAccount, params.Domain)
	}

	options := service.ClientOptions{ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{TryTimeout: params.TryTimeout}}}
	if params.StorageAccessKey != "" {
		cred, err := service.NewSharedKeyCredential(params.StorageAccount, params.StorageAccessKey)
		if err != nil {
			return nil, fmt.Errorf("invalid credentials: %w", err)
		}
		return service.NewClientWithSharedKeyCredential(endpoint, cred, &options)
	}

	defaultCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("missing credentials: %w", err)
	}
	return service.NewClient(endpoint, defaultCreds, &options)
}

func buildAccountEndpoint(storageAccount, domain string) string {
	return fmt.Sprintf("https://%s.%s", storageAccount, domain)
}
