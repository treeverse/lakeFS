package plugins

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
