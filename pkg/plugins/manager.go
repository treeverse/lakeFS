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

// PluginIdentity identifies the plugin name, type, version, and the executable's location.
type PluginIdentity struct {
	Name               string
	Type               string
	Version            int
	ExecutableLocation string
}

// PluginAuth includes authentication properties for the plugin.
type PluginAuth struct {
	Key   string
	Value string
}

// pluginNameConfig includes the plugin's execute command, and its handshake config.
// Examples:
/*
Delta diff example:
{
	HandshakeConfig: plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "deltaKey",
		MagicCookieValue: "deltaValue",
	},
	Cmd: exec.Command{ Path: "path/to/executable/diff/delta", ...}
}

Iceberg merge example
{
	HandshakeConfig: plugin.HandshakeConfig{
		ProtocolVersion:  24,
		MagicCookieKey:   "icebergKey",
		MagicCookieValue: "icebergValue",
	},
	Cmd: exec.Command{ Path: "/path/to/executable/merge/iceberg", ...}
}
*/
type pluginNameConfig struct {
	plugin.HandshakeConfig
	*exec.Cmd
}

// pluginTypeConfig includes the plugin.Plugin interface implementations for the given plugin along with the supported
// protocols and specific configurations for the different implementations for that plugin type.
// Diff Example:
//
//	{
//		AllowedProtocols: []plugin.Protocol{ plugin.ProtocolGRPC },
//		Plugin: map[string]plugin.Plugin{"differPlugin": GRPCPlugin},
//		PluginNameConfigMap: {
//			"delta": <delta pluginNameConfig>,
//			"iceberg: <iceberg pluginNameConfig>,
//		}
//	}
type pluginTypeConfig struct {
	AllowedProtocols    []plugin.Protocol
	Plugins             map[string]plugin.Plugin
	PluginNameConfigMap map[string]pluginNameConfig
}

// The Manager holds the different types of plugins that can be used in the plugin system
type Manager struct {
	pluginTypes map[string]pluginTypeConfig
}

func NewManager() *Manager {
	return &Manager{pluginTypes: make(map[string]pluginTypeConfig)}
}

// LoadClient loads a Client that wraps the go-plugin client.
//
// It uses a plugin's identity: the plugin type and the plugin name under it.
// For example, plugin type = "diff", plugin name = "delta" will generate a Client with a go-plugin client that performs
// diffs over Delta Lake tables.
func (m *Manager) LoadClient(identity PluginIdentity) (*Client, error) {
	if m == nil {
		return nil, ErrUninitializedManager
	}
	ptm, ok := m.pluginTypes[identity.Type]
	if !ok {
		return nil, ErrPluginTypeNotFound
	}
	pluginNameConfig, ok := ptm.PluginNameConfigMap[identity.Name]
	if !ok {
		return nil, ErrPluginNameNotFound
	}

	clientConfig := plugin.ClientConfig{
		Plugins:          ptm.Plugins,
		AllowedProtocols: ptm.AllowedProtocols,
		HandshakeConfig:  pluginNameConfig.HandshakeConfig,
		Cmd:              pluginNameConfig.Cmd,
	}
	return newPluginClient(fmt.Sprintf("%s:%s", identity.Type, identity.Name), clientConfig)
}

// newPluginClient generates a Client that wraps the go-plugin client.
func newPluginClient(clientName string, clientConfig plugin.ClientConfig) (*Client, error) {
	hl := hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("%s_logger", clientName),
		Output: os.Stdout,
		Level:  hclog.Debug,
	})
	l := logging.Default()
	hcl := NewHClogger(hl, l)
	clientConfig.Logger = hcl
	client := plugin.NewClient(&clientConfig)
	return &Client{
		Client: client,
		Log:    l,
	}, nil
}

// RegisterPlugin is used to register a new plugin type with a plugin group. It can also introduce a new plugin group.
func (m *Manager) RegisterPlugin(id PluginIdentity, auth PluginAuth, p plugin.Plugin) error {
	if m == nil {
		return ErrUninitializedManager
	}
	pt, ok := m.pluginTypes[id.Type]
	if !ok {
		pt = typeConfig(id.Type, p)
	}
	pt.PluginNameConfigMap[id.Name] = pluginNameConfig{
		HandshakeConfig: plugin.HandshakeConfig{
			ProtocolVersion:  uint(id.Version),
			MagicCookieKey:   auth.Key,
			MagicCookieValue: auth.Value,
		},
		Cmd: exec.Command(id.ExecutableLocation), //nolint:gosec
	}
	m.pluginTypes[id.Type] = pt
	return nil
}

func typeConfig(pluginType string, p plugin.Plugin) pluginTypeConfig {
	return pluginTypeConfig{
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolGRPC,
		},
		Plugins: map[string]plugin.Plugin{
			pluginType: p,
		},
		PluginNameConfigMap: make(map[string]pluginNameConfig),
	}
}
