package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/treeverse/lakefs/pkg/block/azure"
)

var (
	ErrAzureInvalidURL = errors.New("invalid Azure storage URL")
)

func NewAzureBlobWalker(svc *service.Client) (*azureBlobWalker, error) {
	return &azureBlobWalker{
		client: svc,
		mark:   Mark{HasMore: true},
	}, nil
}

type azureBlobWalker struct {
	client *service.Client
	mark   Mark
}

// extractAzurePrefix takes a URL that looks like this: https://storageaccount.blob.core.windows.net/container/prefix
// and return the URL for the container and a prefix, if one exists
func extractAzurePrefix(storageURI *url.URL) (*url.URL, string, error) {
	path := strings.TrimLeft(storageURI.Path, "/")
	if len(path) == 0 {
		return nil, "", fmt.Errorf("%w: could not parse container URL: %s", ErrAzureInvalidURL, storageURI)
	}
	parts := strings.SplitN(path, "/", 2) // nolint: gomnd
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

func (a *azureBlobWalker) Walk(ctx context.Context, storageURI *url.URL, op WalkOptions, walkFn func(e ObjectStoreEntry) error) error {
	// we use bucket as container and prefix as path
	containerURL, prefix, err := extractAzurePrefix(storageURI)
	if err != nil {
		return err
	}
	qk, err := azure.ResolveBlobURLInfoFromURL(containerURL)
	if err != nil {
		return err
	}

	container := a.client.NewContainerClient(qk.ContainerName)
	listBlob := container.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
		Marker: &op.ContinuationToken,
	})

	for listBlob.More() {
		resp, err := listBlob.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, blobInfo := range resp.Segment.BlobItems {
			// skipping everything in the page which is before 'After' (without forgetting the possible empty string key!)
			if op.After != "" && *blobInfo.Name <= op.After {
				continue
			}
			if resp.Marker != nil {
				a.mark.ContinuationToken = *resp.Marker
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

	a.mark = Mark{
		HasMore: false,
	}

	return nil
}

func (a *azureBlobWalker) Marker() Mark {
	return a.mark
}
