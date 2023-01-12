package plugins

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	errPluginGroupNotFound = fmt.Errorf("unknown plugin group")
	errPluginTypeNotFound  = fmt.Errorf("unknown plugin type")
)

// PluginType specifies the type of related plugins
// For example, "diff" is a type of plugins that know how to perform a diff.
type PluginType string

// PluginName specifies the name of a plugin
// For example, "delta" is a plugin.
type PluginName string

// PluginIdentity identifies the plugin implementation.
type PluginIdentity struct {
	Impl               plugin.Plugin
	ImplName           string
	HandshakeConfig    plugin.HandshakeConfig
	ExecutableLocation string
}
type pluginTypeConfigMap map[PluginName]plugin.ClientConfig

// PluginTypeNameMap example:
/*
{
	"diff": {
		"delta": plugin.clientConfig{
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion:  1,
				MagicCookieKey:   "key",
				MagicCookieValue: "value",
			},
			Cmd: exec.Command("path/to/executable/diff/delta"),
			AllowedProtocols: []plugin.Protocol{
				plugin.ProtocolGRPC,
			},
			Impl: map[string]plugin.Plugin{
				"deltaDiff": DeltaGRPCPlugin,
			},
		},
		"iceberg: {
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion:  1,
				MagicCookieKey:   "key",
				MagicCookieValue: "value",
			},
			Cmd: exec.Command("path/to/executable/diff/iceberg"),
			AllowedProtocols: []plugin.Protocol{
				plugin.ProtocolGRPC,
			},
			Impl: map[string]plugin.Plugin{
				"icebergDiff": IcebergGRPCPlugin
			},
		}
	},
	"merge": {
		"delta": {
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion:  1,
				MagicCookieKey:   "key",
				MagicCookieValue: "value",
			},
			Cmd: exec.Command("path/to/executable/merge/delta"),
			AllowedProtocols: []plugin.Protocol{
				plugin.ProtocolGRPC,
			},
			Impl: map[string]plugin.Plugin{
				"deltaMerge": DeltaGRPCPlugin,
			},
		},
		"iceberg: {
			HandshakeConfig: plugin.HandshakeConfig{
				ProtocolVersion:  1,
				MagicCookieKey:   "key",
				MagicCookieValue: "value",
			},
			Cmd: exec.Command("path/to/executable/merge/iceberg"),
			AllowedProtocols: []plugin.Protocol{
				plugin.ProtocolGRPC,
			},
			Impl: map[string]plugin.Plugin{
				"icebergMerge": IcebergGRPCPlugin
			},
		}
	},
}
*/
type PluginTypeNameMap map[PluginType]pluginTypeConfigMap

// The Manager holds the different types of plugins that can be used in the plugin system
type Manager struct {
	pluginTypes PluginTypeNameMap
}

// WrapPlugin generates a Wrapper that wraps the go-plugin client.
//
// It accepts two parameters: the top PluginType and the required PluginName under it.
// For example, PluginType = "diff", PluginName = "delta" will generate a Wrapper with a client that performs
// diffs over Delta Lake tables.
// It also returns a DestroyClientFunc
func (m *Manager) WrapPlugin(pluginType PluginType, pluginName PluginName) (*Wrapper, error) {
	ptpp, ok := m.pluginTypes[pluginType]
	if !ok {
		return nil, errPluginGroupNotFound
	}
	clientConfig, ok := ptpp[pluginName]
	if !ok {
		return nil, errPluginTypeNotFound
	}
	return newPluginWrapper(fmt.Sprintf("%s_%s", pluginType, pluginName), clientConfig)
}

// newPluginWrapper generates a Wrapper that wraps the go-plugin client.
func newPluginWrapper(clientName string, clientConfig plugin.ClientConfig) (*Wrapper, error) {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("%s_logger", clientName),
		Output: os.Stdout,
		Level:  hclog.Debug,
	})
	clientConfig.Logger = logger
	client := plugin.NewClient(&clientConfig)
	return &Wrapper{
		Client: client,
		Log:    logging.Default(),
	}, nil
}

// RegisterPlugin is used to register a new plugin type with a plugin group. It can also introduce a new plugin group.
func (m *Manager) RegisterPlugin(pluginType PluginType, pluginName PluginName, pluginID PluginIdentity) {
	if m.pluginTypes == nil {
		m.pluginTypes = make(PluginTypeNameMap)
	}
	ptcm, ok := m.pluginTypes[pluginType]
	if !ok {
		ptcm := make(pluginTypeConfigMap)
		m.pluginTypes[pluginType] = ptcm
	}
	ptcm[pluginName] = clientConfig(pluginID)
}

// clientConfig generates a plugin.ClientConfig struct to be used to configure the Manager's plugins with the
// corresponding configurations
func clientConfig(identity PluginIdentity) plugin.ClientConfig {
	handshakeConfig := identity.HandshakeConfig
	allowedProtocols := []plugin.Protocol{
		plugin.ProtocolGRPC,
	}
	return plugin.ClientConfig{
		HandshakeConfig:  handshakeConfig,
		Cmd:              exec.Command(identity.ExecutableLocation), //nolint:gosec
		AllowedProtocols: allowedProtocols,
		// "Plugins": the different types of plugins that the Plugin serves/consumes. Basically a plugin per
		// communication type. There will be a single plugin which is the GRPC implementation of the Plugin.
		Plugins: map[string]plugin.Plugin{
			identity.ImplName: identity.Impl,
		},
	}
}
