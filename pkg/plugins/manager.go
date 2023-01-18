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
	ErrInvalidPluginNotFound = errors.New("invalid plugin type")
	ErrPluginNameNotFound    = errors.New("unknown plugin name")
	ErrUninitializedManager  = errors.New("uninitialized plugins manager")
)

var allowedProtocols = []plugin.Protocol{
	plugin.ProtocolGRPC,
}

// PluginIdentity identifies the plugin's version and executable location.
type PluginIdentity struct {
	Version int
	// ExecutableLocation is the full path to the plugin executable on the local filesystem.
	ExecutableLocation string
}

// PluginHandshake includes handshake properties for the plugin.
type PluginHandshake struct {
	Key   string
	Value string
}

// Manager maps the available plugin client names to the for different kinds of plugin clients.
// Type T is the custom interface type that the plugin clients implement, e.g. "Differ" for plugin clients that
// implement the Differ interface.
// For example, the clients map might contain a mapping of "delta" -> plugin.Client to communicate with the Delta
// plugin.
type Manager[T any] struct {
	clients map[string]*plugin.Client
}

func NewManager[T any]() *Manager[T] {
	return &Manager[T]{
		clients: make(map[string]*plugin.Client),
	}
}

// RegisterPlugin is used to register a new plugin client with the corresponding plugin type.
func (m *Manager[T]) RegisterPlugin(name string, id PluginIdentity, auth PluginHandshake, p plugin.Plugin) error {
	if m == nil {
		return ErrUninitializedManager
	}
	hc := plugin.HandshakeConfig{
		ProtocolVersion:  uint(id.Version),
		MagicCookieKey:   auth.Key,
		MagicCookieValue: auth.Value,
	}
	cmd := exec.Command(id.ExecutableLocation) //nolint:gosec
	c, err := newPluginClient(name, p, hc, cmd)
	if err != nil {
		return err
	}
	_, ok := any(c).(T)
	if !ok {
		return ErrInvalidPluginNotFound
	}
	m.clients[name] = c
	return nil
}

func newPluginClient(name string, p plugin.Plugin, hc plugin.HandshakeConfig, cmd *exec.Cmd) (*plugin.Client, error) {
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
	return plugin.NewClient(&clientConfig), nil
}

// LoadPluginClient loads a Client of type T.
func (m *Manager[T]) LoadPluginClient(name string) (*T, error) {
	if m == nil {
		return nil, ErrUninitializedManager
	}
	c, ok := m.clients[name]
	if !ok {
		return nil, ErrPluginNameNotFound
	}
	grpcClient, err := c.Client()
	if err != nil {
		return nil, err
	}
	stub, err := grpcClient.Dispense(name)
	if err != nil {
		return nil, err
	}
	cd, _ := stub.(T)
	return &cd, nil
}
