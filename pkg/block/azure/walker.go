package azure

import (
	"context"
	"errors"
	"fmt"
	"github.com/treeverse/lakefs/pkg/logging"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/treeverse/lakefs/pkg/block"
)

var ErrAzureInvalidURL = errors.New("invalid Azure storage URL")

func NewAzureBlobWalker(svc *service.Client) (*BlobWalker, error) {
	return &BlobWalker{
		client: svc,
		mark:   block.Mark{HasMore: true},
	}, nil
}

type BlobWalker struct {
	client *service.Client
	mark   block.Mark
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

func (a *BlobWalker) Walk(ctx context.Context, storageURI *url.URL, op block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	// we use bucket as container and prefix as path
	containerURL, prefix, err := extractAzurePrefix(storageURI)
	if err != nil {
		return err
	}
	var basePath string
	if idx := strings.LastIndex(prefix, "/"); idx != -1 {
		basePath = prefix[:idx+1]
	}

	qk, err := ResolveBlobURLInfoFromURL(containerURL)
	if err != nil {
		return err
	}

	containerClient := a.client.NewContainerClient(qk.ContainerName)
	listBlob := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
		Marker: &op.ContinuationToken,
	})

	for listBlob.More() {
		resp, err := listBlob.NextPage(ctx)
		if err != nil {
			return err
		}
		if resp.Marker != nil {
			a.mark.ContinuationToken = *resp.Marker
		}
		for _, blobInfo := range resp.Segment.BlobItems {
			// skipping everything in the page which is before 'After' (without forgetting the possible empty string key!)
			if op.After != "" && *blobInfo.Name <= op.After {
				continue
			}
			a.mark.LastKey = *blobInfo.Name
			if err := walkFn(block.ObjectStoreEntry{
				FullKey:     *blobInfo.Name,
				RelativeKey: strings.TrimPrefix(*blobInfo.Name, basePath),
				Address:     getAzureBlobURL(containerURL, *blobInfo.Name).String(),
				ETag:        string(*blobInfo.Properties.ETag),
				Mtime:       *blobInfo.Properties.LastModified,
				Size:        *blobInfo.Properties.ContentLength,
			}); err != nil {
				return err
			}
		}
	}

	a.mark = block.Mark{
		HasMore: false,
	}

	return nil
}

func (a *BlobWalker) Marker() block.Mark {
	return a.mark
}

func (a *BlobWalker) GetSkippedEntries() []block.ObjectStoreEntry {
	return nil
}

//
// DataLakeWalker
//

func NewAzureDataLakeWalker(svc *service.Client) (*DataLakeWalker, error) {
	return &DataLakeWalker{
		client: svc,
		mark:   block.Mark{HasMore: true},
	}, nil
}

type DataLakeWalker struct {
	client  *service.Client
	mark    block.Mark
	skipped []block.ObjectStoreEntry
}

func (a *DataLakeWalker) Walk(ctx context.Context, storageURI *url.URL, op block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	// we use bucket as container and prefix as path
	containerURL, prefix, err := extractAzurePrefix(storageURI)
	if err != nil {
		return err
	}
	var basePath string
	if idx := strings.LastIndex(prefix, "/"); idx != -1 {
		basePath = prefix[:idx+1]
	}

	qk, err := ResolveBlobURLInfoFromURL(containerURL)
	if err != nil {
		return err
	}

	containerClient := a.client.NewContainerClient(qk.ContainerName)
	listBlob := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
		Marker: &op.ContinuationToken,
	})

	skipCount := 0
	prev := ""
	for listBlob.More() {
		resp, err := listBlob.NextPage(ctx)
		if err != nil {
			return err
		}
		if resp.Marker != nil {
			a.mark.ContinuationToken = *resp.Marker
		}
		for _, blobInfo := range resp.Segment.BlobItems {
			// skipping everything in the page which is before 'After' (without forgetting the possible empty string key!)
			if op.After != "" && *blobInfo.Name <= op.After {
				continue
			}

			if *blobInfo.Properties.ContentLength == 0 && blobInfo.Properties.ContentMD5 == nil {
				// Skip folders
				continue
			}
			if strings.Compare(prev, *blobInfo.Name) > 0 { // skip out of order
				a.skipped = append(a.skipped, block.ObjectStoreEntry{
					FullKey:     *blobInfo.Name,
					RelativeKey: strings.TrimPrefix(*blobInfo.Name, basePath),
					Address:     getAzureBlobURL(containerURL, *blobInfo.Name).String(),
					ETag:        string(*blobInfo.Properties.ETag),
					Mtime:       *blobInfo.Properties.LastModified,
					Size:        *blobInfo.Properties.ContentLength,
				})
				skipCount++
				continue
			}
			prev = *blobInfo.Name

			a.mark.LastKey = *blobInfo.Name
			if err := walkFn(block.ObjectStoreEntry{
				FullKey:     *blobInfo.Name,
				RelativeKey: strings.TrimPrefix(*blobInfo.Name, basePath),
				Address:     getAzureBlobURL(containerURL, *blobInfo.Name).String(),
				ETag:        string(*blobInfo.Properties.ETag),
				Mtime:       *blobInfo.Properties.LastModified,
				Size:        *blobInfo.Properties.ContentLength,
			}); err != nil {
				return err
			}
		}
	}
	logging.Default().Warning("Skipped count:", skipCount)
	a.mark = block.Mark{
		HasMore: false,
	}

	return nil
}

func (a *DataLakeWalker) Marker() block.Mark {
	return a.mark
}

func (a *DataLakeWalker) GetSkippedEntries() []block.ObjectStoreEntry {
	return a.skipped
}
