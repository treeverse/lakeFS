package azure

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/puzpuzpuz/xsync"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/params"
)

type ClientContainerCache struct {
	containerToClient *xsync.MapOf[string, *container.Client]
	params            params.Azure
}

var ErrUnknownAzureURL = errors.New("unable to parse storage account")

func NewCache(p params.Azure) *ClientContainerCache {
	return &ClientContainerCache{
		containerToClient: xsync.NewMapOf[*container.Client](),
		params:            p,
	}
}

func ExtractStorageAccount(storageAccount *url.URL) (string, error) {
	// In azure the subdomain is the storage account
	const expectedHostParts = 2
	hostParts := strings.SplitN(storageAccount.Host, ".", expectedHostParts)
	if len(hostParts) != expectedHostParts {
		return "", fmt.Errorf("wrong host parts(%d): %w", len(hostParts), block.ErrInvalidNamespace)
	}

	return hostParts[0], nil
}

func mapKey(storageAccount, containerName string) string {
	return fmt.Sprintf("%s#%s", storageAccount, containerName)
}

func (c *ClientContainerCache) NewContainerClient(storageAccount, containerName string) (*container.Client, error) {
	p := c.params
	p.StorageAccount = storageAccount
	key := mapKey(storageAccount, containerName)

	var err error
	cl, _ := c.containerToClient.LoadOrCompute(key, func() *container.Client {
		var svc *service.Client
		svc, err = BuildAzureServiceClient(p)
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

func BuildAzureServiceClient(params params.Azure) (*service.Client, error) {
	url := fmt.Sprintf(URLTemplate, params.StorageAccount)
	options := service.ClientOptions{ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{TryTimeout: params.TryTimeout}}}
	if params.StorageAccessKey != "" {
		cred, err := service.NewSharedKeyCredential(params.StorageAccount, params.StorageAccessKey)
		if err != nil {
			return nil, fmt.Errorf("invalid credentials: %w", err)
		}
		return service.NewClientWithSharedKeyCredential(url, cred, &options)
	}

	defaultCreds, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("missing credentials: %w", err)
	}
	return service.NewClient(url, defaultCreds, &options)
}
