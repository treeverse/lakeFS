package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/block/azure"
)

var (
	ErrAzureInvalidURL  = errors.New("invalid Azure storage URL")
	ErrAzureCredentials = errors.New("azure credentials error")
)

func getAzureClient() (*service.Client, error) {
	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		return nil, fmt.Errorf("%w: either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set", ErrAzureCredentials)
	}

	// Create a default request client using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("invalid credentials with error: %w", err)
	}
	containerURL := fmt.Sprintf(azure.AzURLTemplate, accountName)
	return service.NewClientWithSharedKeyCredential(containerURL, credential, nil)
}

func NewAzureBlobWalker(svc service.Client) (*azureBlobWalker, error) {
	return &azureBlobWalker{
		client: svc,
		mark:   Mark{HasMore: true},
	}, nil
}

type azureBlobWalker struct {
	client service.Client
	mark   Mark
}

// extractAzurePrefix takes a URL that looks like this: https://storageaccount.blob.core.windows.net/container/prefix
// and return the URL for the container and a prefix, if one exists
func extractAzurePrefix(storageURI *url.URL) (*url.URL, string, error) {
	path := strings.TrimLeft(storageURI.Path, "/")
	if len(path) == 0 {
		return nil, "", fmt.Errorf("%w: could not parse container URL: %s", ErrAzureInvalidURL, storageURI)
	}
	parts := strings.SplitN(path, "/", 2) //nolint: gomnd
	if len(parts) == 1 {
		// we only have a container
		return storageURI, "", nil
	}
	// we have both prefix and storage container, rebuild URL
	relativePath := url.URL{Path: "/" + parts[0]}
	return storageURI.ResolveReference(&relativePath), parts[1], nil
}

func getAzureBlobURL(containerURL *url.URL, blobName string) *url.URL {
	relativePath := url.URL{Path: containerURL.Path + "/" + blobName}
	return containerURL.ResolveReference(&relativePath)
}

func (a *azureBlobWalker) Walk(_ context.Context, storageURI *url.URL, op WalkOptions, walkFn func(e ObjectStoreEntry) error) error {
	// we use bucket as container and prefix as path
	containerURL, prefix, err := extractAzurePrefix(storageURI)
	if err != nil {
		return err
	}
	qk, err := azure.ResolveBlobURLInfoFromURL(containerURL)
	container := a.client.NewContainerClient(qk.ContainerName)

	for marker := &op.ContinuationToken; marker != nil; {
		listBlob := container.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
			Prefix: &prefix,
			Marker: marker,
		})

		for listBlob.More() {
			resp, err := listBlob.NextPage(context.Background())
			if err != nil {
				return err
			}
			a.mark.ContinuationToken = swag.StringValue(marker)
			marker = resp.NextMarker
			for _, blobInfo := range resp.Segment.BlobItems {
				// skipping everything in the page which is before 'After' (without forgetting the possible empty string key!)
				if op.After != "" && *blobInfo.Name <= op.After {
					continue
				}
				a.mark.LastKey = *blobInfo.Name
				if err := walkFn(ObjectStoreEntry{
					FullKey:     *blobInfo.Name,
					RelativeKey: strings.TrimPrefix(*blobInfo.Name, prefix),
					Address:     getAzureBlobURL(containerURL, *blobInfo.Name).String(),
					ETag:        string(*blobInfo.Properties.ETag),
					Mtime:       *blobInfo.Properties.LastModified,
					Size:        *blobInfo.Properties.ContentLength,
				}); err != nil {
					return err
				}
			}
		}
	}

	a.mark = Mark{
		HasMore: false,
	}

	return nil
}

func (a *azureBlobWalker) Marker() Mark {
	return a.mark
}
