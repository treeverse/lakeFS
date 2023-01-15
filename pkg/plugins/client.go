package plugins

import (
	"errors"

	"github.com/hashicorp/go-plugin"
)

var ErrClientAlreadyClosed = errors.New("client already closed")

// Client wraps the go-plugin client (which is in fact a plugin controller) and the GRPC client.
// It's used as a convenience struct to call the different types of plugins, and to be included as a subtype of specific
// plugin client implementations
type Client struct {
	gRPCClient *plugin.ClientProtocol
	client     *plugin.Client
	log        HClogger
}

// NewClient constructs a new Client instance
func NewClient(client *plugin.Client, protocol *plugin.ClientProtocol, log HClogger) Client {
	return Client{
		gRPCClient: protocol,
		client:     client,
		log:        log,
	}
}

// Close terminates the plugin
func (c Client) Close() error {
	if c.client == nil {
		return ErrClientAlreadyClosed
	}
	c.client.Kill()
	return nil
}
