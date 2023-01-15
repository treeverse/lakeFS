package plugins

import (
	"errors"

	"github.com/hashicorp/go-plugin"
	"github.com/treeverse/lakefs/pkg/logging"
)

var ErrClientAlreadyInitialized = errors.New("gRPC client already initialized")
var ErrClientAlreadyClosed = errors.New("client already closed")

// Client wraps the go-plugin client (which is in fact a plugin controller) and the GRPC client.
// It's used as a convenience struct to call the different types of plugins, and to be included as a subtype of specific
// plugin client implementations
type Client struct {
	GRPCClient *plugin.ClientProtocol
	Client     *plugin.Client
	Log        logging.Logger
}

// InitClient initializes the inner GRPC client to communicate to the
func (c *Client) InitClient() error {
	if c.GRPCClient != nil {
		return ErrClientAlreadyInitialized
	}
	grpcClient, err := c.Client.Client()
	if err != nil {
		return err
	}
	c.GRPCClient = &grpcClient
	return nil
}

// Close terminates the plugin
func (c *Client) Close() error {
	if c.Client == nil {
		return ErrClientAlreadyClosed
	}
	c.Client.Kill()
	return nil
}
