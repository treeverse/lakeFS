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

{{- if .Verbose }}
  {{- range .Plugins }}
  {{ .Path }}
  {{- end }}
{{- else }}
  {{- range .Plugins }}
  {{ .Name }}
  {{- end }}
{{- end }}
`

type pluginInfo struct {
	Name string
	Path string
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
				dir = "."
			}
			if _, ok := seen[dir]; !ok {
				seen[dir] = struct{}{}
				pathDirs = append(pathDirs, dir)
			}
		}

		// Iterate through pathDirs to build the final list with correct overshadowing
		pluginsOutput := make([]pluginInfo, 0)
		foundEffectivePlugins := make(map[string]string) // plugin name -> path of chosen executable
		const pluginPrefix = "lakectl-"

		for _, dir := range pathDirs {
			files, err := os.ReadDir(dir)
			if err != nil {
				if verbose && !os.IsNotExist(err) {
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

				fullPath := filepath.Join(dir, fileName)
				absPath, err := filepath.Abs(fullPath)
				if err != nil {
					if verbose {
						Warning(fmt.Sprintf("getting absolute path for %s - %s", fullPath, err))
					}
					continue
				}

				fileInfo, statErr := file.Info()
				if statErr != nil {
					if verbose {
						Warning(fmt.Sprintf("getting file info for %s - %s", fullPath, statErr))
					}
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

				// Skip non-executable files
				if !isExec {
					if verbose {
						Warning(fmt.Sprintf("%s is not executable", absPath))
					}
					continue
				}

				// Check if this is the first executable plugin found for this name
				if existingPath, ok := foundEffectivePlugins[pluginName]; ok {
					if verbose {
						Warning(fmt.Sprintf("found multiple executables for plugin %s: %s and %s", pluginName, existingPath, absPath))
					}
					continue
				}
				foundEffectivePlugins[pluginName] = absPath

				pluginsOutput = append(pluginsOutput, pluginInfo{Name: pluginName, Path: absPath})
			}
		}

		// Sort the final list by path for consistent output
		slices.SortFunc(pluginsOutput, func(a, b pluginInfo) int {
			return strings.Compare(a.Path, b.Path)
		})

		// Use the standard Write function for template execution
		Write(pluginListTemplate, struct {
			Verbose bool
			Plugins []pluginInfo
		}{
			Verbose: verbose,
			Plugins: pluginsOutput,
		})
	},
}

//nolint:gochecknoinits
func init() {
	pluginListCmd.Flags().BoolP("verbose", "v", false, "show full path for all plugins")
	pluginCmd.AddCommand(pluginListCmd)
}
