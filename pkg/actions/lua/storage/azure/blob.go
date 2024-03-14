package azure

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/block/azure"
	"github.com/treeverse/lakefs/pkg/block/params"
	"github.com/treeverse/lakefs/pkg/uri"
)

type Client struct {
	ctx    context.Context
	client *service.Client
}

func newBlobClient(ctx context.Context) lua.Function {
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

		l.NewTable()
		functions := map[string]lua.Function{
			"get_object":    client.GetObject,
			"put_object":    client.PutObject,
			"delete_object": client.DeleteObject,
		}
		for name, goFn := range functions {
			l.PushGoFunction(goFn)
			l.SetField(-2, name)
		}
		return 1
	}
}

func (c *Client) GetObject(l *lua.State) int {
	path := lua.CheckString(l, 1)
	containerName, key := parsePath(l, path)
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
	path := lua.CheckString(l, 1)
	buf := strings.NewReader(lua.CheckString(l, 2))
	containerName, key := parsePath(l, path)
	blobClient := c.getBlobClient(containerName, key)
	_, err := blobClient.UploadStream(c.ctx, buf, &azblob.UploadStreamOptions{})
	if err != nil {
		lua.Errorf(l, "azure client: (container %s) (key %s): %s", containerName, key, err.Error())
		panic("unreachable")
	}
	return 0
}

func (c *Client) DeleteObject(l *lua.State) int {
	path := lua.CheckString(l, 1)
	containerName, key := parsePath(l, path)
	blobClient := c.getBlobClient(containerName, key)
	_, err := blobClient.Delete(c.ctx, nil)
	if err != nil {
		lua.Errorf(l, "%s", err.Error())
		panic("unreachable")
	}
	return 0
}

func (c *Client) getBlobClient(container, blob string) *blockblob.Client {
	return c.getContainerClient(container).NewBlockBlobClient(blob)
}

func (c *Client) getContainerClient(container string) *container.Client {
	return c.client.NewContainerClient(container)
}

func parsePath(l *lua.State, path string) (string, string) {
	// Extract containerName and key from path
	containerName, key, found := strings.Cut(path, uri.PathSeparator)
	if !found {
		lua.Errorf(l, "azure client: invalid path, missing container name from path: %s", path)
		panic("unreachable")
	}
	return containerName, key
}

func transformPathToAbfss(l *lua.State) int {
	path := lua.CheckString(l, 1)
	const numOfParts = 3
	r := regexp.MustCompile(`^https://(\w+)\.blob\.core\.windows\.net/([^/]*)/(.+)$`)
	parts := r.FindStringSubmatch(path)
	if len(parts) != numOfParts+1 {
		lua.Errorf(l, "expected valid Azure https URL: %s", path)
		panic("unreachable")
	}
	transformed := fmt.Sprintf("abfss://%s@%s.dfs.core.windows.net/%s", parts[2], parts[1], parts[3])
	l.PushString(transformed)
	return 1
}
