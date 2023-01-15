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

// PluginNameConfig includes the plugin's execute command, and its handshake config.
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
type PluginNameConfig struct {
	plugin.HandshakeConfig
	*exec.Cmd
}

// PluginTypeConfig includes the plugin.Plugin interfaces for the given plugin along with the supported protocols
// and specific configurations for the different implementations for that plugin type.
// Diff Example:
//
//	{
//		AllowedProtocols: []plugin.Protocol{ plugin.ProtocolGRPC },
//		Plugin: map[string]plugin.Plugin{"differPlugin": GRPCPlugin},
//		PluginNameConfigMap: {
//			"delta": <delta PluginNameConfig>,
//			"iceberg: <iceberg PluginNameConfig>,
//		}
//	}
type PluginTypeConfig struct {
	AllowedProtocols    []plugin.Protocol
	Plugins             map[string]plugin.Plugin
	PluginNameConfigMap map[string]PluginNameConfig
}

// The Manager holds the different types of plugins that can be used in the plugin system
type Manager struct {
	pluginTypes map[string]PluginTypeConfig
}

func NewPluginsManager() *Manager {
	return &Manager{pluginTypes: make(map[string]PluginTypeConfig)}
}

// WrapPlugin generates a Wrapper that wraps the go-plugin client.
//
// It a plugin's identity: the plugin type and the plugin name under it.
// For example, plugin type = "diff", plugin name = "delta" will generate a Wrapper with a client that performs
// diffs over Delta Lake tables.
func (m *Manager) WrapPlugin(identity PluginIdentity) (*Wrapper, error) {
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
	return newPluginWrapper(fmt.Sprintf("%s:%s", identity.Type, identity.Name), clientConfig)
}

// newPluginWrapper generates a Wrapper that wraps the go-plugin client.
func newPluginWrapper(clientName string, clientConfig plugin.ClientConfig) (*Wrapper, error) {
	hl := hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("%s_logger", clientName),
		Output: os.Stdout,
		Level:  hclog.Debug,
	})
	l := logging.Default()
	hcl := NewHClogger(hl, l)
	clientConfig.Logger = hcl
	client := plugin.NewClient(&clientConfig)
	return &Wrapper{
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
		pt = pluginTypeConfig(id.Type, p)
	}
	pt.PluginNameConfigMap[id.Name] = PluginNameConfig{
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

func pluginTypeConfig(pluginType string, p plugin.Plugin) PluginTypeConfig {
	return PluginTypeConfig{
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolGRPC,
		},
		Plugins: map[string]plugin.Plugin{
			pluginType: p,
		},
		PluginNameConfigMap: make(map[string]PluginNameConfig),
	}
}

// clientConfig generates a plugin.ClientConfig struct to be used to configure the Manager's plugins with the
// corresponding configurations.
func (m *Manager) clientConfig(pluginType string, pluginName string) (*plugin.ClientConfig, error) {
	if m == nil {
		return nil, ErrUninitializedManager
	}
	ptc, ok := m.pluginTypes[pluginType]
	if !ok {
		return nil, ErrPluginTypeNotFound
	}
	pnc, ok := ptc.PluginNameConfigMap[pluginName]
	if !ok {
		return nil, ErrPluginNameNotFound
	}
	return &plugin.ClientConfig{
		HandshakeConfig:  pnc.HandshakeConfig,
		Cmd:              pnc.Cmd,
		AllowedProtocols: ptc.AllowedProtocols,
		Plugins:          ptc.Plugins,
	}, nil
}
