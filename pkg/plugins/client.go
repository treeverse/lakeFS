package plugins

import (
	"errors"

	"github.com/hashicorp/go-plugin"
)

var ErrClientAlreadyClosed = errors.New("client already closed")

// Client wraps the go-plugin client (which is in fact a plugin controller) and the GRPC client.
type Client struct {
	client *plugin.Client
	log    HClogger
}

// NewClient constructs a new Client instance
func NewClient(client *plugin.Client, log HClogger) Client {
	return Client{
		client: client,
		log:    log,
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
