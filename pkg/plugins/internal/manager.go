package internal

import (
	"errors"
	"os"
	"os/exec"

	"github.com/treeverse/lakefs/pkg/plugins"

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

// Handler is an interface used to handle the different plugin implementation.
// T is the custom interface that the plugins implement.
// I is the type of input properties needed to support the plugin.
// Look at Manager to get an example of an implementation for this interface.
type Handler[T, I any] interface {
	RegisterPlugin(string, I)
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

type HCPluginProperties struct {
	ID        plugins.PluginIdentity
	Handshake plugins.PluginHandshake
	P         plugin.Plugin
}

// RegisterPlugin registers a new plugin client with the corresponding plugin type.
func (m *Manager[T]) RegisterPlugin(name string, props HCPluginProperties) {
	id := props.ID

	hc := plugin.HandshakeConfig{
		ProtocolVersion:  id.ProtocolVersion,
		MagicCookieKey:   props.Handshake.Key,
		MagicCookieValue: props.Handshake.Value,
	}
	cmd := exec.Command(id.ExecutableLocation, id.ExecutableArgs...) // #nosec G204
	cmd.Env = id.ExecutableEnvVars
	c := newPluginClient(name, props.P, hc, cmd)
	cp := &HCPluginProperties{
		ID:        id,
		Handshake: props.Handshake,
		P:         props.P,
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
		// The re registration occurs because in some cases the issue is with the binary itself.
		// If this is the case, the user will be able to fix the issue with the plugin binary without needing to
		// re-initialize lakeFS.
		m.closeAndReRegisterClient(name)
		return ans, nil, err
	}
	rawGrpcClientStub, err := grpcPluginClient.Dispense(name) // Returns an implementation of the stub service.
	if err != nil {
		m.closePluginClient(name)
		return ans, nil, err
	}
	ans, ok := rawGrpcClientStub.(T)
	if !ok {
		m.pluginApplicationClients.Remove(name)
		return ans, nil, ErrPluginOfWrongType
	}
	return ans, func() {
		m.closeAndReRegisterClient(name)
	}, nil
}

func (m *Manager[T]) closeAndReRegisterClient(name string) {
	cp := m.closePluginClient(name)
	m.RegisterPlugin(name, *cp)
}

func (m *Manager[T]) closePluginClient(name string) *HCPluginProperties {
	c, cp, _ := m.pluginApplicationClients.Client(name)
	c.Kill()
	return cp
}
