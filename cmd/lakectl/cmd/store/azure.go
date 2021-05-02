package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

var (
	ErrAzureBlobMisconfigured = errors.New("Either the AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_ACCESS_KEY environment variable is not set")
)

func GetAzureClient() (pipeline.Pipeline, error) {
	// From the Azure portal, get your storage account name and key and set environment variables.
	accountName, accountKey := os.Getenv("AZURE_STORAGE_ACCOUNT"), os.Getenv("AZURE_STORAGE_ACCESS_KEY")
	if len(accountName) == 0 || len(accountKey) == 0 {
		return nil, ErrAzureBlobMisconfigured
	}

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("Invalid credentials with error: %s", err.Error())
	}
	return azblob.NewPipeline(credential, azblob.PipelineOptions{}), nil
}

type AzureBlobWalker struct {
	client pipeline.Pipeline
}

func extractAzurePrefix(storageURI *url.URL) (*url.URL, string, error) {
	// take a URL that looks like this: https://storageaccount.blob.core.windows.net/container/prefix
	// and return the URL for the container, and the URL for the prefix if any
	path := strings.TrimLeft(storageURI.Path, "/")
	if len(path) == 0 {
		return nil, "", fmt.Errorf("invalid storage URI: could not parse container: %s", storageURI)
	}
	parts := strings.SplitN(path, "/", 2)
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

func (a *AzureBlobWalker) Walk(ctx context.Context, storageURI *url.URL, walkFn func(e ObjectStoreEntry) error) error {
	// we use bucket as container and prefix as path
	containerURL, prefix, err := extractAzurePrefix(storageURI)
	if err != nil {
		return err
	}
	container := azblob.NewContainerURL(*containerURL, a.client)
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := container.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{Prefix: prefix})
		if err != nil {
			return err
		}
		marker = listBlob.NextMarker
		for _, blobInfo := range listBlob.Segment.BlobItems {
			if err := walkFn(ObjectStoreEntry{
				FullKey:     blobInfo.Name,
				RelativeKey: strings.TrimPrefix(blobInfo.Name, prefix),
				Address:     getAzureBlobURL(containerURL, blobInfo.Name).String(),
				ETag:        string(blobInfo.Properties.Etag),
				Mtime:       blobInfo.Properties.LastModified,
				Size:        *blobInfo.Properties.ContentLength,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
