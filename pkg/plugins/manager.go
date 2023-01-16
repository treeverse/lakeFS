package plugins

import (
	"errors"
	"fmt"
	"os"
	"os/exec"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	ErrPluginTypeNotFound   = errors.New("unknown plugin type")
	ErrPluginNameNotFound   = errors.New("unknown plugin name")
	ErrUninitializedManager = errors.New("uninitialized plugins manager")
)

type PluginType int

const (
	Diff PluginType = iota
)

var allowedProtocols = []plugin.Protocol{
	plugin.ProtocolGRPC,
}

// PluginIdentity identifies the plugin's version and executable location.
type PluginIdentity struct {
	Version            int
	ExecutableLocation string
}

// PluginAuth includes authentication properties for the plugin.
type PluginAuth struct {
	Key   string
	Value string
}

// The Manager holds different maps for different kinds of possible plugin clients.
// For example, the diffClients map might contain a mapping of "delta" -> plugin.Client to communicate with the Delta
// plugin.
type Manager struct {
	diffClients map[string]*plugin.Client
}

func NewManager() *Manager {
	return &Manager{
		diffClients: make(map[string]*plugin.Client),
	}
}

// RegisterPlugin is used to register a new plugin client with the corresponding plugin type.
func (m *Manager) RegisterPlugin(name string, id PluginIdentity, auth PluginAuth, pt PluginType, p plugin.Plugin) error {
	if m == nil {
		return ErrUninitializedManager
	}
	switch pt {
	case Diff:
		return m.registerDiffPlugin(name, id, auth, p)
	default:
		return ErrPluginTypeNotFound
	}
}

func (m *Manager) registerDiffPlugin(name string, id PluginIdentity, auth PluginAuth, p plugin.Plugin) error {
	hc := plugin.HandshakeConfig{
		ProtocolVersion:  uint(id.Version),
		MagicCookieKey:   auth.Key,
		MagicCookieValue: auth.Value,
	}
	cmd := exec.Command(id.ExecutableLocation) //nolint:gosec
	c, err := pluginClient(name, p, hc, cmd)
	if err != nil {
		return err
	}
	m.diffClients[name] = c
	return nil
}

func pluginClient(name string, p plugin.Plugin, hc plugin.HandshakeConfig, cmd *exec.Cmd) (*plugin.Client, error) {
	clientConfig := plugin.ClientConfig{
		Plugins: map[string]plugin.Plugin{
			name: p,
		},
		AllowedProtocols: allowedProtocols,
		HandshakeConfig:  hc,
		Cmd:              cmd,
	}
	return newPluginClient(name, clientConfig)
}

func newPluginClient(clientName string, clientConfig plugin.ClientConfig) (*plugin.Client, error) {
	hl := hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("%s_logger", clientName),
		Output: os.Stdout,
		Level:  hclog.Debug,
	})
	l := logging.Default()
	hcl := NewHClogger(hl, l)
	clientConfig.Logger = hcl
	return plugin.NewClient(&clientConfig), nil
}

// LoadDiffPluginClient loads a Client that wraps the go-plugin client.
//
// It uses a plugin's identity: the plugin type and the plugin name under it.
// For example, plugin type = "diff", plugin name = "delta" will generate a Client with a go-plugin client that performs
// diffs over Delta Lake tables.
// TODO: return type should be the "Diff" interface we'll define, and we should assert the correct type of 'stub'
// and return it.
func (m *Manager) LoadDiffPluginClient(name string) (interface{}, error) {
	if m == nil {
		return nil, ErrUninitializedManager
	}
	c, ok := m.diffClients[name]
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
	return stub, nil
}
