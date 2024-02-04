package azure

import (
	"context"
	"io"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/storage"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/uri"
)

type Client struct {
	ctx    context.Context
	client *service.Client
}

func newClient(ctx context.Context) lua.Function {
	return func(l *lua.State) int {
		storageAccount := lua.CheckString(l, 1)
		accessKey := lua.CheckString(l, 2)

		azClient, err := azure.BuildAzureServiceClient(params.Azure{
			StorageAccount:   storageAccount,
			StorageAccessKey: accessKey,
		})
		if err != nil {
			panic(err)
		}

		client := &Client{
			ctx:    ctx,
			client: azClient,
		}

		storage.InitStorageClient(l, client)
		return 1
	}
}

func (c *Client) GetObject(l *lua.State) int {
	containerName := lua.CheckString(l, 1)
	key := lua.CheckString(l, 2)
	blobClient := c.getBlobClient(containerName, key)
	downloadResponse, err := blobClient.DownloadStream(c.ctx, &azblob.DownloadStreamOptions{})

	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			l.PushString("")
			l.PushBoolean(false) // exists
			return 2
		}
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}

	data, err := io.ReadAll(downloadResponse.Body)
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	l.PushString(string(data))
	l.PushBoolean(true) // exists
	return 2
}

func (c *Client) PutObject(l *lua.State) int {
	// Skipping first argument as it is the host arg (bucket which is irrelevant in Azure)
	path := lua.CheckString(l, 2)
	buf := strings.NewReader(lua.CheckString(l, 3))
	// Extract containerName and key from path
	containerName, key, found := strings.Cut(path, uri.PathSeparator)
	if !found {
		lua.Errorf(l, "azure client: invalid path, missing container name from path: %s", path)
		panic("unreachable")
	}
	blobClient := c.getBlobClient(containerName, key)
	_, err := blobClient.UploadStream(c.ctx, buf, &azblob.UploadStreamOptions{})
	if err != nil {
		lua.Errorf(l, "azure client: (container %s) (key %s): %s", containerName, key, err.Error())
		panic("unreachable")
	}
	return 0
}

func (c *Client) DeleteObject(l *lua.State) int {
	containerName := lua.CheckString(l, 1)
	key := lua.CheckString(l, 2)
	blobClient := c.getBlobClient(containerName, key)
	_, err := blobClient.Delete(c.ctx, nil)
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	return 0
}

// ListObjects Should be implemented when needed. There are nuances between HNS and BlobStorage which requires understanding the
// Actual use case before implementing the solution
func (c *Client) ListObjects(l *lua.State) int {
	lua.Errorf(l, "Not implemented")
	panic("unreachable")
}

// DeleteRecursive Should be implemented when needed. There are nuances between HNS and BlobStorage which requires understanding the
// Actual use case before implementing the solution
func (c *Client) DeleteRecursive(l *lua.State) int {
	lua.Errorf(l, "Not implemented")
	panic("unreachable")
}

func (c *Client) getBlobClient(container, blob string) *blockblob.Client {
	return c.getContainerClient(container).NewBlockBlobClient(blob)
}

func (c *Client) getContainerClient(container string) *container.Client {
	return c.client.NewContainerClient(container)
}
