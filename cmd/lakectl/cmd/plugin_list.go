package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

var pluginListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available lakectl plugins",
	Long:  `Scans the PATH for executables named "lakectl-*" and lists the detected plugins.`,
	Run: func(cmd *cobra.Command, args []string) {
		envPathDirs := filepath.SplitList(os.Getenv("PATH"))

		// Filter out duplicates from envPathDirs
		var pathDirs []string
		seen := make(map[string]struct{})
		for _, dir := range envPathDirs {
			if dir == "" {
				dir = "."
			}
			if _, ok := seen[dir]; !ok {
				seen[dir] = struct{}{}
				pathDirs = append(pathDirs, dir)
			}
		}

		// Iterate through pathDirs to build the final list with correct overshadowing
		pluginsOutput := make([]string, 0)
		foundPlugins := make(map[string]struct{})
		const pluginPrefix = "lakectl-"

		for _, dir := range pathDirs {
			files, err := os.ReadDir(dir)
			if err != nil {
				if !os.IsNotExist(err) {
					Warning(fmt.Sprintf("reading directory %s - %s", dir, err))
				}
				continue
			}

			for _, file := range files {
				fileName := file.Name()
				if !strings.HasPrefix(fileName, pluginPrefix) {
					continue
				}
				pluginName := strings.TrimPrefix(fileName, pluginPrefix)
				if pluginName == "" {
					continue
				}

				fileInfo, statErr := file.Info()
				if statErr != nil {
					fullPath := filepath.Join(dir, fileName)
					Warning(fmt.Sprintf("getting file info for %s - %s", fullPath, statErr))
					continue
				}
				if fileInfo.IsDir() {
					continue
				}

				// Skip non-executable files
				var isExec bool
				if runtime.GOOS == "windows" {
					// On Windows, check if the file is executable by checking its extension
					ext := strings.ToLower(filepath.Ext(fileName))
					isExec = ext == ".exe" || ext == ".com" || ext == ".bat" || ext == ".cmd"
				} else {
					// On Unix-like systems, check if the file is executable by user, group, or others
					isExec = fileInfo.Mode().Perm()&0o111 != 0 //nolint:mnd
				}
				if !isExec {
					continue
				}

				// Skip if we already found a plugin with the same name
				if _, ok := foundPlugins[pluginName]; ok {
					continue
				}
				foundPlugins[pluginName] = struct{}{}
				pluginsOutput = append(pluginsOutput, pluginName)
			}
		}

		// Sort the final list by path for consistent output
		sort.Strings(pluginsOutput)

		// Use the standard Write function for template execution
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
