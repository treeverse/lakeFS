package internal

import (
	"errors"
	"os"
	"os/exec"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	ErrPluginOfWrongType = errors.New("plugin of the wrong type")
	ErrPluginNotFound    = errors.New("unknown plugin Client")
)

var allowedProtocols = []plugin.Protocol{
	plugin.ProtocolGRPC,
}

// PluginIdentity identifies the plugin's version and executable location.
type PluginIdentity struct {
	ProtocolVersion uint
	// ExecutableLocation is the full path to the plugin server's executable location
	ExecutableLocation string
	// ExecutableArgs is the argument list for the provided plugin executable - optional
	ExecutableArgs []string
	// ExecutableEnvVars is the environment variable list for the provided plugin executable - optional
	ExecutableEnvVars []string
}

// PluginHandshake includes handshake properties for the plugin.
type PluginHandshake struct {
	Key   string
	Value string
}

type Controller[T any] interface {
	RegisterPlugin(string, PluginIdentity, PluginHandshake, plugin.Plugin)
	LoadPluginClient(string) (T, func(), error)
}

// Manager holds a clientStore and is responsible to register and unregister `plugin.Client`s, and to load
// the underlying GRPC Client.
// T is the custom interface type that the returned GRPC Client implementation implements, e.g. "Differ" for `plugin.Client`s that
//
//	include a GRPCClient that implements the "Differ" interface:
//	grpcPluginClient, err := c.Client() // Returns a plugin.GRPCClient
//	rawGrpcClientStub, err := grpcPluginClient.Dispense(name) // Calls grpcPluginClient's GRPCClient method and returns the gRPC stub.
//	grpcClient, ok := rawGrpcClientStub.(Differ) // Asserts the expected type of stub client.
type Manager[T any] struct {
	pluginApplicationClients *clientStore
}

func NewManager[T any]() *Manager[T] {
	return &Manager[T]{
		pluginApplicationClients: newClientsMap(),
	}
}

// RegisterPlugin registers a new plugin client with the corresponding plugin type.
func (m *Manager[T]) RegisterPlugin(name string, id PluginIdentity, auth PluginHandshake, p plugin.Plugin) {
	hc := plugin.HandshakeConfig{
		ProtocolVersion:  id.ProtocolVersion,
		MagicCookieKey:   auth.Key,
		MagicCookieValue: auth.Value,
	}
	cmd := exec.Command(id.ExecutableLocation, id.ExecutableArgs...) // #nosec G204
	cmd.Env = id.ExecutableEnvVars
	c := newPluginClient(name, p, hc, cmd)
	cp := &clientProps{
		ID:   id,
		Auth: auth,
		P:    p,
	}
	m.pluginApplicationClients.Insert(name, c, cp)
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

// LoadPluginClient returns a GRPCClient which also implements the custom plugin interface T.
// The returned function is used to close the Client and reset it. The reset is needed because after
// plugin.Client.Client() is called, it's internally creating channels that are closed when we Kill the Client,
// and won't allow to run the Client again.
// The close function should be called when the plugin is no longer of use.
func (m *Manager[T]) LoadPluginClient(name string) (T, func(), error) {
	var ans T
	c, _, err := m.pluginApplicationClients.Client(name)
	if err != nil {
		return ans, nil, err
	}
	grpcPluginClient, err := c.Client()
	if err != nil {
		return ans, nil, err
	}
	rawGrpcClientStub, err := grpcPluginClient.Dispense(name) // Returns an implementation of the stub service.
	if err != nil {
		return ans, nil, err
	}
	ans, ok := rawGrpcClientStub.(T)
	if !ok {
		m.pluginApplicationClients.Remove(name)
		return ans, nil, ErrPluginOfWrongType
	}
	return ans, func() {
		cp := m.closePluginClient(name)
		m.RegisterPlugin(name, cp.ID, cp.Auth, cp.P)
	}, nil
}

func (m *Manager[T]) closePluginClient(name string) *clientProps {
	c, cp, _ := m.pluginApplicationClients.Client(name)
	c.Kill()
	return cp
}
