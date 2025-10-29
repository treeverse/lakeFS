package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

var pluginListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available lakectl plugins",
	Long:  `Scans the PATH for executables named "lakectl-*" and lists the detected plugins.`,
	Run: func(cmd *cobra.Command, args []string) {
		pathDirs := filepath.SplitList(os.Getenv("PATH"))

		// Iterate through pathDirs to build a list of possible names for plugins.
		// To avoid duplication of the executable lookup code we will verify each one later that it can be executed.
		possibleNames := make(map[string]struct{})
		pathSeen := make(map[string]struct{})
		const pluginPrefix = "lakectl-"

		for _, dir := range pathDirs {
			if dir == "" {
				dir = "."
			}
			// Skip if we've already seen this directory
			if _, ok := pathSeen[dir]; ok {
				continue
			}
			pathSeen[dir] = struct{}{}

			// Read directory content and collect potential plugin names
			files, err := os.ReadDir(dir)
			if err != nil {
				if !os.IsNotExist(err) {
					Warning(fmt.Sprintf("reading directory %s - %s", dir, err))
				}
				continue
			}

			for _, file := range files {
				fileName := file.Name()
				// Skip if the file name does not start with the plugin prefix and is not long enough to have a name
				if len(fileName) <= len(pluginPrefix) || !strings.HasPrefix(fileName, pluginPrefix) {
					continue
				}

				fullPath := filepath.Join(dir, fileName)
				fileInfo, statErr := file.Info()
				if statErr != nil {
					Warning(fmt.Sprintf("getting file info for %s - %s", fullPath, statErr))
					continue
				}
				if fileInfo.IsDir() {
					continue
				}
				possibleNames[fileName] = struct{}{}
			}
		}

		// Now verify each possible name and check if it can be executed using LookPath
		pluginsOutput := make([]string, 0)
		for name := range possibleNames {
			_, err := exec.LookPath(name)
			if err != nil {
				continue
			}
			pluginsOutput = append(pluginsOutput, strings.TrimPrefix(name, pluginPrefix))
		}
		if len(pluginsOutput) == 0 {
			cmd.Printf("No lakectl plugins found in your PATH.\n")
			return
		}

		// Sort the final list by path for consistent output
		sort.Strings(pluginsOutput)
		cmd.Printf("Available plugins:\n")
		for _, plugin := range pluginsOutput {
			cmd.Printf("  %s\n", plugin)
		}
	},
}

//nolint:gochecknoinits
func init() {
	pluginCmd.AddCommand(pluginListCmd)
}
