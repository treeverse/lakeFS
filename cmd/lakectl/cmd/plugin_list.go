package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

const pluginListTemplate = `The following lakectl-compatible plugins are available:

{{- range .Plugins }}
  {{- if or .Verbose .Warning }}
  {{ .Path }}
  {{- else }}
  {{ .Name }}
  {{- end }}
  {{- if .Warning }}
    - {{ "warning:" | yellow }} {{ .Warning }}
  {{- end }}
{{- end }}
`

type pluginInfo struct {
	Name    string
	Path    string
	Warning string
	Verbose bool
}

var pluginListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available lakectl plugins",
	Long:  `Scans the PATH for executables named "lakectl-*" and lists the detected plugins.`,
	Run: func(cmd *cobra.Command, args []string) {
		verbose, _ := cmd.Flags().GetBool("verbose")
		envPathDirs := filepath.SplitList(os.Getenv("PATH"))

		// Filter out duplicates from envPathDirs
		var pathDirs []string
		seen := make(map[string]struct{})
		for _, dir := range envPathDirs {
			if dir == "" {
				// Skip empty entries in PATH
				continue
			}
			if _, ok := seen[dir]; !ok {
				seen[dir] = struct{}{}
				pathDirs = append(pathDirs, dir)
			}
		}

		// Iterate through pathDirs to build the final list with correct overshadowing
		pluginsOutput := make([]pluginInfo, 0)
		foundEffectivePlugins := make(map[string]string) // plugin name -> path of chosen executable

		for _, dir := range pathDirs {
			files, err := os.ReadDir(dir)
			if err != nil {
				// Silently ignore errors from non-existent or unreadable directories in PATH
				continue
			}

			for _, file := range files {
				fileName := file.Name()
				if !strings.HasPrefix(fileName, "lakectl-") {
					continue
				}
				pluginName := strings.TrimPrefix(fileName, "lakectl-")
				if pluginName == "" {
					continue
				}

				fullPath := filepath.Join(dir, fileName)
				absPath, err := filepath.Abs(fullPath)
				if err != nil {
					// Silently ignore errors from file paths that cannot be resolved
					continue
				}

				fileInfo, statErr := file.Info()
				if statErr != nil {
					pluginsOutput = append(pluginsOutput, pluginInfo{Name: pluginName, Path: fullPath, Warning: fmt.Sprintf("error getting file info: %v", statErr), Verbose: verbose})
					continue
				}
				if fileInfo.IsDir() {
					continue
				}

				var isExec bool
				if runtime.GOOS == "windows" {
					// On Windows, check if the file is executable by checking its extension
					ext := strings.ToLower(filepath.Ext(fileName))
					isExec = ext == ".exe" || ext == ".com" || ext == ".bat" || ext == ".cmd"
				} else {
					// On Unix-like systems, check if the file is executable by user, group, or others
					isExec = fileInfo.Mode().Perm()&0111 != 0 //nolint:mnd
				}

				currentPI := pluginInfo{Name: pluginName, Path: absPath, Verbose: verbose}
				if !isExec {
					currentPI.Warning = "identified as a lakectl plugin, but it is not executable"
				}

				if existingPath, ok := foundEffectivePlugins[pluginName]; ok {
					// Already found an effective plugin for this name. This one is overshadowed.
					if currentPI.Warning != "" { // It has its own issue (e.g. not executable)
						currentPI.Warning = fmt.Sprintf("%s (also overshadowed by %s)", currentPI.Warning, existingPath)
					} else {
						currentPI.Warning = fmt.Sprintf("overshadowed by %s", existingPath)
					}
				} else if isExec {
					// This is the first executable plugin found for this name.
					foundEffectivePlugins[pluginName] = absPath
				}

				pluginsOutput = append(pluginsOutput, currentPI)
			}
		}

		// Sort the final list by path for consistent output
		slices.SortFunc(pluginsOutput, func(a, b pluginInfo) int {
			return strings.Compare(a.Path, b.Path)
		})

		// Use the standard Write function for template execution
		Write(pluginListTemplate, struct {
			Plugins []pluginInfo
		}{
			Plugins: pluginsOutput,
		})
	},
}

//nolint:gochecknoinits
func init() {
	pluginListCmd.Flags().BoolP("verbose", "v", false, "show full path for all plugins")
	pluginCmd.AddCommand(pluginListCmd)
}
