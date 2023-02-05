package plugins

import (
	"errors"
	"os"
	"os/exec"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	ErrPluginOfWrongType  = errors.New("plugin of the wrong type")
	ErrPluginNameNotFound = errors.New("unknown plugin name")
)

var allowedProtocols = []plugin.Protocol{
	plugin.ProtocolGRPC,
}

// PluginIdentity identifies the plugin's version and executable location.
type PluginIdentity struct {
	ProtocolVersion uint
	// Cmd is the command that is used to run the plugin executable on the local filesystem.
	Cmd exec.Cmd
}

// PluginHandshake includes handshake properties for the plugin.
type PluginHandshake struct {
	Key   string
	Value string
}

// Manager maps the available plugin names to the different kinds of plugin.Client plugin controllers.
// T is the custom interface type that the returned GRPCClient implementation implements, e.g. "Differ" for `plugin.Client`s that
//
//	include a GRPCClient that implements the "Differ" interface:
//	grpcPluginClient, err := c.Client() // Returns a plugin.GRPCClient
//	rawGrpcClientStub, err := grpcPluginClient.Dispense(name) // Calls grpcPluginClient's GRPCClient method and returns the gRPC stub.
//	grpcClient, ok := rawGrpcClientStub.(Differ) // Asserts the expected type of stub client.
//
// The map might include a mapping of "delta" -> plugin.Client to communicate with the Delta plugin.
type Manager[T any] struct {
	pluginApplicationClients map[string]*plugin.Client
}

func NewManager[T any]() *Manager[T] {
	return &Manager[T]{
		pluginApplicationClients: make(map[string]*plugin.Client),
	}
}

// RegisterPlugin registers a new plugin client with the corresponding plugin type.
func (m *Manager[T]) RegisterPlugin(name string, id PluginIdentity, auth PluginHandshake, p plugin.Plugin) {
	hc := plugin.HandshakeConfig{
		ProtocolVersion:  id.ProtocolVersion,
		MagicCookieKey:   auth.Key,
		MagicCookieValue: auth.Value,
	}
	cmd := id.Cmd
	c := newPluginClient(name, p, hc, &cmd)
	m.pluginApplicationClients[name] = c
}

func newPluginClient(name string, p plugin.Plugin, hc plugin.HandshakeConfig, cmd *exec.Cmd) *plugin.Client {
	clientConfig := plugin.ClientConfig{
		Plugins: map[string]plugin.Plugin{
			name: p,
		},
		AllowedProtocols: allowedProtocols,
		HandshakeConfig:  hc,
		Cmd:              cmd,
	}
	hl := hclog.New(&hclog.LoggerOptions{
		Name:   name,
		Output: os.Stdout,
		Level:  hclog.Debug,
	})
	l := logging.Default()
	hcl := NewHClogger(hl, l)
	clientConfig.Logger = hcl
	return plugin.NewClient(&clientConfig)
}

// LoadPluginClient loads a Client of type T.
func (m *Manager[T]) LoadPluginClient(name string) (T, error) {
	var zero T
	c, ok := m.pluginApplicationClients[name]
	if !ok {
		return zero, ErrPluginNameNotFound
	}
	grpcPluginClient, err := c.Client()
	if err != nil {
		return zero, err
	}
	rawGrpcClientStub, err := grpcPluginClient.Dispense(name) // Returns an implementation of the stub service.
	if err != nil {
		return zero, err
	}
	stub, ok := rawGrpcClientStub.(T)
	if !ok {
		delete(m.pluginApplicationClients, name)
		return zero, ErrPluginOfWrongType
	}
	return stub, nil
}
