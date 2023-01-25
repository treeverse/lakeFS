package azure

import (
	"errors"
	"fmt"
	"regexp"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/puzpuzpuz/xsync"
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

// storageAccountRegex is used to extract the storage account from azure storage url.
// Following rules from https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview#storage-account-name
var storageAccountRegex = regexp.MustCompile("https://([a-z0-9]{3,24}).blob.core.windows.net/.*")

func ExtractStorageAccount(storageAccountURL string) (string, error) {
	matches := storageAccountRegex.FindAllStringSubmatch(storageAccountURL, -1)
	if len(matches) != 1 || len(matches[0]) != 2 {
		return "", ErrUnknownAzureURL
	}
	return matches[0][1], nil
}

func mapKey(storageAccount, containerName string) string {
	return fmt.Sprintf("%s#%s", storageAccount, containerName)
}

func (c *ClientContainerCache) NewContainerClient(storageAccountURL, containerName string) (*container.Client, error) {
	storageAccount, err := ExtractStorageAccount(storageAccountURL)
	if err != nil {
		return nil, err
	}
	p := c.params
	p.StorageAccount = storageAccount

	key := mapKey(storageAccount, containerName)
	if err != nil {
		return nil, err
	}

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
