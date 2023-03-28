package azure

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
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
	client *service.Client
	mark   block.Mark
}

func (a *DataLakeWalker) Walk(ctx context.Context, storageURI *url.URL, op block.WalkOptions, walkFn func(e block.ObjectStoreEntry) error) error {
	// We are limiting the ADLS Gen2 walker to traverse directories only. This is to avoid a bug where we traverse the parent folder as well
	// due to the use of NewListBlobsHierarchyPager
	storageURI = storageURI.JoinPath("/")
	// we use bucket as container and prefix as path
	containerURL, prefix, err := extractAzurePrefix(storageURI)
	if err != nil {
		return err
	}
	basePath := prefix

	qk, err := ResolveBlobURLInfoFromURL(containerURL)
	if err != nil {
		return err
	}

	// restore state - tokens and prefix based on after
	tokens := strings.Split(op.ContinuationToken, ",")
	if len(tokens) == 0 {
		tokens = append(tokens, "")
	}

	// use a copy of after as we update the value though while listing different levels
	after := op.After
	if after != "" {
		prefix = parentPrefix(after)
	}

	containerClient := a.client.NewContainerClient(qk.ContainerName)
	for {
	LIST:
		tokenIdx := len(tokens) - 1
		l := containerClient.NewListBlobsHierarchyPager("/",
			&container.ListBlobsHierarchyOptions{
				Prefix: &prefix,
				Marker: &tokens[tokenIdx],
			})
		for l.More() {
			resp, err := l.NextPage(ctx)
			if err != nil {
				return err
			}
			if resp.Marker != nil {
				tokens[tokenIdx] = *resp.Marker
			}
			a.mark.ContinuationToken = strings.Join(tokens, ",")

			itemIdx, prefIdx := 0, 0
			items, prefixes := resp.Segment.BlobItems, resp.Segment.BlobPrefixes
			for itemIdx < len(items) || prefIdx < len(prefixes) {
				var f *container.BlobItem
				if itemIdx < len(items) {
					f = items[itemIdx]
				}

				var p *container.BlobPrefix
				if prefIdx < len(prefixes) {
					p = prefixes[prefIdx]
				}

				// process file or prefix
				if f != nil && (p == nil || *f.Name < *p.Name) {
					itemIdx++
					// skip until we pass 'after'
					if after != "" && *f.Name <= after {
						continue
					}
					a.mark.LastKey = *f.Name
					after = *f.Name
					if err := walkFn(block.ObjectStoreEntry{
						FullKey:     *f.Name,
						RelativeKey: strings.TrimPrefix(*f.Name, basePath),
						Address:     getAzureBlobURL(containerURL, *f.Name).String(),
						ETag:        string(*f.Properties.ETag),
						Mtime:       *f.Properties.LastModified,
						Size:        *f.Properties.ContentLength,
					}); err != nil {
						return err
					}
				} else if p != nil && (f == nil || *p.Name < *f.Name) {
					prefIdx++
					// skip until we pass 'after'
					if after != "" && *p.Name <= after {
						continue
					}
					// process prefix
					tokens = append(tokens, "")
					prefix = *p.Name
					after = *p.Name
					goto LIST
				}
			}
		}
		tokens = tokens[:tokenIdx]
		// no tokens, means done walking
		if len(tokens) == 0 {
			break
		}
		prefix = parentPrefix(prefix)
	}
	a.mark = block.Mark{
		HasMore: false,
	}
	return nil
}

func parentPrefix(prefix string) string {
	prefix = strings.TrimSuffix(prefix, "/")
	if i := strings.LastIndex(prefix, "/"); i != -1 {
		return prefix[:i+1]
	}
	return ""
}

func (a *DataLakeWalker) Marker() block.Mark {
	return a.mark
}
