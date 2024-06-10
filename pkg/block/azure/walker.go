package azure

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/treeverse/lakefs/pkg/block"
)

const DirectoryBlobMetadataKey = "hdi_isfolder"

var ErrAzureInvalidURL = errors.New("invalid Azure storage URL")

// extractAzurePrefix takes a URL that looks like this: https://storageaccount.blob.core.windows.net/container/prefix
// and return the URL for the container and a prefix, if one exists
func extractAzurePrefix(storageURI *url.URL) (*url.URL, string, error) {
	path := strings.TrimLeft(storageURI.Path, "/")
	if len(path) == 0 {
		return nil, "", fmt.Errorf("%w: could not parse container URL: %s", ErrAzureInvalidURL, storageURI)
	}
	parts := strings.SplitN(path, "/", 2) // nolint: mnd
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

// isBlobItemFolder returns true if the blob item is a folder.
// Make sure that metadata is populated before calling this function.
// Example: for listing using blob API passing options with `Include: container.ListBlobsInclude{ Metadata: true }`
// will populate the metadata.
func isBlobItemFolder(blobItem *container.BlobItem) bool {
	if blobItem.Metadata == nil {
		return false
	}
	if blobItem.Properties.ContentLength != nil && *blobItem.Properties.ContentLength != 0 {
		return false
	}
	isFolder, ok := blobItem.Metadata[DirectoryBlobMetadataKey]
	if !ok || isFolder == nil {
		return false
	}
	return *isFolder == "true"
}

// extractBlobItemEtag etag set by content md5 with fallback to use Etag value
func extractBlobItemEtag(blobItem *container.BlobItem) string {
	if blobItem.Properties.ContentMD5 != nil {
		return hex.EncodeToString(blobItem.Properties.ContentMD5)
	}
	if blobItem.Properties.ETag != nil {
		etag := string(*blobItem.Properties.ETag)
		return strings.TrimFunc(etag, func(r rune) bool { return r == '"' || r == ' ' })
	}
	return ""
}

//
// DataLakeWalker
//

func NewAzureDataLakeWalker(svc *service.Client, skipOutOfOrder bool) (*DataLakeWalker, error) {
	return &DataLakeWalker{
		client:         svc,
		mark:           block.Mark{HasMore: true},
		skipOutOfOrder: skipOutOfOrder,
	}, nil
}

type DataLakeWalker struct {
	client         *service.Client
	mark           block.Mark
	skipped        []block.ObjectStoreEntry
	skipOutOfOrder bool
}

func (a *DataLakeWalker) Walk(ctx context.Context, storageURI *url.URL, op block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	// we use bucket as container and prefix as a path
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
		Include: container.ListBlobsInclude{
			Metadata: true,
		},
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

			// Skip folders
			if isBlobItemFolder(blobInfo) {
				continue
			}

			entry := block.ObjectStoreEntry{
				FullKey:     *blobInfo.Name,
				RelativeKey: strings.TrimPrefix(*blobInfo.Name, basePath),
				Address:     getAzureBlobURL(containerURL, *blobInfo.Name).String(),
				ETag:        extractBlobItemEtag(blobInfo),
				Mtime:       *blobInfo.Properties.LastModified,
				Size:        *blobInfo.Properties.ContentLength,
			}
			if a.skipOutOfOrder && strings.Compare(prev, *blobInfo.Name) > 0 { // skip out of order
				a.skipped = append(a.skipped, entry)
				skipCount++
				continue
			}
			prev = *blobInfo.Name

			a.mark.LastKey = *blobInfo.Name
			if err := walkFn(entry); err != nil {
				return err
			}
		}
	}
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
