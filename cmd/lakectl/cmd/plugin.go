package cmd

import (
	"github.com/spf13/cobra"
)

// pluginCmd represents the plugin command
var pluginCmd = &cobra.Command{
	Use:   "plugin",
	Short: "Manage lakectl plugins",
	Long: `Provides utilities for managing lakectl plugins.

Plugins are standalone executable files that extend lakectl's functionality.
lakectl discovers plugins by looking for executables in your PATH
that are named with the prefix "lakectl-".

For example, an executable named "lakectl-myfeature" can be invoked as
"lakectl myfeature [args...]".

Plugin Naming:
  - The executable must start with "lakectl-".
  - The part after "lakectl-" becomes the command name users type.
    (e.g., "lakectl-foo" -> "lakectl foo")
  - The plugin name is used exactly as-is in the command.
    (e.g., "lakectl-foo-bar" -> "lakectl foo-bar")

Installation:
  - Place your "lakectl-..." executable file (which may be any executable,
    e.g. a Python application) in a directory listed in your PATH.
  - Ensure the file has execute permissions.

Execution:
  - When you run "lakectl some-plugin arg1 --flag", lakectl searches for
    "lakectl-some-plugin" in PATH.
  - If found and executable, it runs the plugin, passing "arg1 --flag" as arguments.
  - The plugin inherits environment variables from lakectl.
  - Standard output, standard error, and the exit code of the plugin are propagated.
  - Built-in lakectl commands always take precedence over plugins.

Use "lakectl plugin list" to see all detected plugins and any warnings.
`,
}

//nolint:gochecknoinits
func init() {
	rootCmd.AddCommand(pluginCmd)
}
