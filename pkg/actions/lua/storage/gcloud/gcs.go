package gcloud

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/Shopify/go-lua"
	"github.com/treeverse/lakefs/pkg/actions/lua/path"
	"github.com/treeverse/lakefs/pkg/actions/lua/util"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const (
	// googleAuthCloudPlatform - Cloud Storage authentication https://cloud.google.com/storage/docs/authentication
	googleAuthCloudPlatform = "https://www.googleapis.com/auth/cloud-platform"
)

var ErrInvalidGCSURI = errors.New("invalid Google Cloud Storage URI")

func Open(l *lua.State, ctx context.Context) {
	open := func(l *lua.State) int {
		lua.NewLibrary(l, []lua.RegistryFunction{
			{Name: "gs_client", Function: newGSClient(ctx)},
		})
		return 1
	}
	lua.Require(l, "gcloud", open, false)
	l.Pop(1)
}

func newGSClient(ctx context.Context) lua.Function {
	return func(l *lua.State) int {
		json := lua.CheckString(l, 1)
		c := &GSClient{
			JSON: json,
			ctx:  ctx,
		}
		l.NewTable()
		for name, goFn := range functions {
			// -1: tbl
			l.PushGoFunction(goFn(c))
			// -1: fn, -2:tbl
			l.SetField(-2, name)
		}
		return 1
	}
}

type GSClient struct {
	JSON string
	ctx  context.Context
}

func (c *GSClient) client() (*storage.Client, error) {
	cred, err := google.CredentialsFromJSON(c.ctx, []byte(c.JSON), googleAuthCloudPlatform)
	if err != nil {
		return nil, err
	}
	return storage.NewClient(c.ctx, option.WithCredentials(cred))
}

var functions = map[string]func(client *GSClient) lua.Function{
	"write_fuse_symlink": writeFuseSymlink,
}

func writeFuseSymlink(c *GSClient) lua.Function {
	return func(l *lua.State) int {
		// convert the relative physical address with the mount point
		physicalAddress := lua.CheckString(l, 1)
		outputAddress := lua.CheckString(l, 2)
		mountInfo, err := util.PullStringTable(l, 3)
		if err != nil {
			lua.Errorf(l, "could not read mount info: %s", err.Error())
		}

		// let's resolve the path:
		if fromValue, removeFrom := mountInfo["from"]; removeFrom {
			physicalAddress = strings.TrimPrefix(physicalAddress, fromValue)
		}
		if toValue, prependTo := mountInfo["to"]; prependTo {
			physicalAddress = path.Join("/", toValue, physicalAddress)
		}

		// write the object
		client, err := c.client()
		if err != nil {
			lua.Errorf(l, "could not initialize google storage client: %s", err.Error())
		}
		defer func() { _ = client.Close() }()

		obj, err := asObject(client, outputAddress)
		if err != nil {
			lua.Errorf(l, "could not parse destination object \"%s\": %s", outputAddress, err.Error())
		}

		w := obj.NewWriter(c.ctx)
		w.Metadata = map[string]string{
			"gcsfuse_symlink_target": physicalAddress,
		}
		err = w.Close()
		if err != nil {
			lua.Errorf(l, "could not close object \"%s\": %s", outputAddress, err.Error())
		}
		return 0
	}
}

func asObject(client *storage.Client, gsURI string) (*storage.ObjectHandle, error) {
	parsed, err := url.Parse(gsURI)
	if err != nil {
		return nil, fmt.Errorf("unable to parse URI: %s: %w", gsURI, ErrInvalidGCSURI)
	}
	if parsed.Scheme != "gs" {
		return nil, ErrInvalidGCSURI
	}
	bucket := parsed.Host
	objectPath := strings.TrimPrefix(parsed.Path, path.SEPARATOR)
	return client.Bucket(bucket).Object(objectPath), nil
}
