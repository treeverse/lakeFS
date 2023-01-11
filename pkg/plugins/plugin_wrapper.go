package plugins

import (
	"github.com/hashicorp/go-plugin"
	"github.com/treeverse/lakefs/pkg/logging"
)

// PluginWrapper wraps the go-plugin client (which is in fact a plugin controller) and the GRPC client.
// It's used as a convenience struct to call the different types of plugins, and to be included as a subtype of specific
// plugin client implementations
type PluginWrapper struct {
	GRPCClient *plugin.ClientProtocol
	Client     *plugin.Client
	Log        logging.Logger
}

// InitClient initializes the inner GRPC client to communicate to the
func (c *PluginWrapper) InitClient() error {
	if c.GRPCClient != nil {
		return nil
	}
	grpcClient, err := c.Client.Client()
	if err != nil {
		c.Log.Fatal(err)
		return err
	}
	c.GRPCClient = &grpcClient
	return nil
}

// DestroyClient terminates the plugin
func (c *PluginWrapper) DestroyClient() {
	if c.Client != nil {
		c.Client.Kill()
	}
}
