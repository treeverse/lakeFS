package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/flare"
)

const (
	flareOutputDirTemplate     = "%s/flare/%s/"
	flareConfigFileName        = "lakefs-config.yaml"
	flareDefaultEnvVarFileName = "lakefs-env.txt"
	flareDefaultZipFileName    = "lakefs-flare.zip"
	flareDefaultOutputPath     = "."
	envVarAppPrefix            = "LAKEFS_"
)

var (
	packageContents      bool   = false
	includeEnvVars       bool   = true
	outputPath           string = flareDefaultOutputPath
	envVarOutputFileName string = flareDefaultEnvVarFileName
	zipOutputFileName    string = flareDefaultZipFileName
	outputStdout         bool   = false
)

var flareCmd = &cobra.Command{
	Use:    "flare",
	Short:  "collect configuration, environment variables, and logs for debugging and troubleshooting",
	Args:   cobra.ExactArgs(0),
	PreRun: warnOutputFlags,
	Run: func(cmd *cobra.Command, args []string) {
		if runtime.GOOS != "windows" {
			syscall.Umask(flare.FlareUmask)
		}
		now := strings.ReplaceAll(time.Now().String(), " ", "")
		cfg := loadConfig()
		envVarBlacklist := addAppEnvVarPrefix(config.GetSecureStringKeyPaths(cfg))
		flr, err := flare.NewFlare(flare.WithEnvVarBlacklist(envVarBlacklist))
		if err != nil {
			printMsgAndExit("failed to create flare instance", err)
		}

		flarePath := fmt.Sprintf(flareOutputDirTemplate, outputPath, now)
		err = os.MkdirAll(flarePath, flare.DirPermissions)
		if err != nil {
			msg := fmt.Sprintf("failed to create flare directory at %s", flarePath)
			printMsgAndExit(msg, err)
		}
		var ow flare.FlareOutputWriter = &flare.FileWriter{}
		if packageContents {
			ow, err = flare.NewZipWriter(filepath.Join(flarePath, zipOutputFileName))
		}
		if outputStdout {
			ow = &flare.StdoutWriter{}
		}
		defer func() {
			e := ow.Close()
			if e != nil {
				printMsgAndExit("failed to write zip file", err)
			}
		}()
		if err != nil {
			printMsgAndExit("failed to create zip writer", err)
		}
		printInfo("Processing config...")
		err = flr.ProcessConfig(cfg, flarePath, flareConfigFileName, ow.GetFileWriter)
		if err != nil {
			printNonFatalError("failed to process config ", err)
		}
		printInfo("Done processing config")

		if includeEnvVars {
			printInfo("Processing env vars...")
			err = flr.ProcessEnvVars(flarePath, envVarOutputFileName, ow.GetFileWriter)
			if err != nil {
				printNonFatalError("failed to process env vars ", err)
			}
			printInfo("Done processing env vars")
		}
	},
}

func warnOutputFlags(cmd *cobra.Command, args []string) {
	if outputStdout && packageContents {
		fmt.Fprint(os.Stderr, text.FgHiYellow.Sprint("Warning: Stdout output is set. Package contents flag will be ignored.\n"))
	}

	if outputStdout && (cmd.Flags().Changed("output") || cmd.Flags().Changed("env-var-filename") || cmd.Flags().Changed("zip-filename")) {
		fmt.Fprint(os.Stderr, text.FgHiYellow.Sprint("Warning: Stdout output is set. File output related flags will be ignored.\n"))
	}
}

func addAppEnvVarPrefix(keys []string) []string {
	prefixedKeys := make([]string, 0, len(keys))
	for _, k := range keys {
		prefixedKeys = append(prefixedKeys, fmt.Sprintf("%s%s", envVarAppPrefix, k))
	}
	return prefixedKeys
}

//nolint:gochecknoinits
func init() {
	flareCmd.Flags().BoolVarP(&packageContents, "package", "p", false, "Package generated artifacts into a .zip file. Default: false")
	flareCmd.Flags().BoolVar(&includeEnvVars, "include-env-vars", true, "Collect environment variables. Default: true")
	flareCmd.Flags().StringVarP(&outputPath, "output", "o", flareDefaultOutputPath, "Output path relative to the current path")
	flareCmd.Flags().StringVar(&envVarOutputFileName, "env-var-filename", flareDefaultEnvVarFileName, "The name of the file to which env vars will be written")
	flareCmd.Flags().StringVar(&zipOutputFileName, "zip-filename", flareDefaultZipFileName, "The file name of the output zip archive")
	flareCmd.Flags().BoolVar(&outputStdout, "stdout", false, "output to Stdout")
	rootCmd.AddCommand(flareCmd)
}
