package cmd

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/flare"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	flareFilePath              = "%s/flare/%s/"
	flareConfigFileName        = "lakefs-config.yaml"
	flareDefaultEnvVarFileName = "lakefs-env.txt"
	flareDefaultZipFileName    = "lakefs-flare.zip"
	flareDefaultOutputPath     = "."
)

var (
	startLogDate         string
	endLogDate           string
	packageContents      bool   = false
	includeLogs          bool   = true
	includeEnvVars       bool   = true
	outputPath           string = flareDefaultOutputPath
	envVarOutputFileName string = flareDefaultEnvVarFileName
	zipOutputFileName    string = flareDefaultZipFileName
)

var flareCmd = &cobra.Command{
	Use:   "flare",
	Short: "collect configuration, environment variables, and logs for debugging and troubleshooting",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		syscall.Umask(flare.FlareUmask)
		now := time.Now().String()
		cfg := loadConfig()
		logFormat := flare.LogFormat(cfg.Logging.Format)
		if !flare.SupportedLogFormat(string(logFormat)) {
			printMsgAndExit(fmt.Sprintf("unsupported log file format: %s ", cfg.Logging.Format))
		}
		flr, err := flare.NewFlare(logFormat)
		if err != nil {
			printMsgAndExit("failed to create flare instance", err)
		}
		parsedStartLogDate, parsedEndLogDate := preflightValidations(cfg, flr)

		flarePath := fmt.Sprintf(flareFilePath, outputPath, now)
		err = os.MkdirAll(flarePath, flare.DirPermissions)
		if err != nil {
			msg := fmt.Sprintf("failed to create flare directory at %s", flarePath)
			printMsgAndExit(msg, err)
		}

		err = flr.ProcessConfig(cfg, flarePath, flareConfigFileName)
		if err != nil {
			printMsgAndExit("failed to process config", err)
		}

		if includeLogs {
			logFilePath := logging.GetLogFileOutputPath(cfg.Logging.Output)

			err = flr.ProcessLogFiles(
				logFilePath,
				flarePath,
				parsedStartLogDate,
				parsedEndLogDate,
			)
			if err != nil {
				printMsgAndExit("failed to process log file ", err)
			}
		}

		if includeEnvVars {
			err = flr.ProcessEnvVars(flarePath, envVarOutputFileName)
			if err != nil {
				printMsgAndExit("failed to process env vars ", err)
			}
		}

		if packageContents {
			err = flr.ZipFolder(flarePath, zipOutputFileName)
			if err != nil {
				printMsgAndExit("failed to package contents", err)
			}
		}
	},
}

func printMsgAndExit(params ...any) {
	fmt.Println("Error:", fmt.Sprint(params...))
	os.Exit(1)
}

func preflightValidations(cfg *config.Config, flr *flare.Flare) (*time.Time, *time.Time) {
	hasFileOutput := logging.HasLogFileOutput(cfg.Logging.Output)
	if !hasFileOutput {
		printMsgAndExit("lakefs isn't configured to output logs to a file. ")
	}

	start, err := validateAndParseDateFlags(startLogDate, flr.LogDateLayout)
	if err != nil {
		printMsgAndExit("failed parsing start date flag ", err)
	}
	end, err := validateAndParseDateFlags(endLogDate, flr.LogDateLayout)
	if err != nil {
		printMsgAndExit("failed parsing end date flag ", err)
	}
	return start, end
}

func validateAndParseDateFlags(dateFlag, dateLayout string) (*time.Time, error) {
	if dateFlag == "" {
		return nil, nil
	}

	parsedDate, err := time.Parse(dateLayout, dateFlag)
	if err != nil {
		return nil, err
	}
	return &parsedDate, nil
}

//nolint:gochecknoinits
func init() {
	flareCmd.Flags().StringVarP(&startLogDate, "log-start-date", "s", "", "Start date of logs to include in the ISO 8601 format")
	flareCmd.Flags().StringVarP(&endLogDate, "log-end-date", "e", "", "End date of logs to include in the ISO 8601 format")
	flareCmd.Flags().BoolVarP(&packageContents, "package", "p", false, "Package generated artifacts into a .zip file. Default: false")
	flareCmd.Flags().BoolVar(&includeLogs, "include-logs", true, "Collect logs. Default: true")
	flareCmd.Flags().BoolVar(&includeEnvVars, "include-env-vars", true, "Collect environment variables. Default: true")
	flareCmd.Flags().StringVarP(&outputPath, "output", "o", flareDefaultOutputPath, "Output path relative to the current path")
	flareCmd.Flags().StringVar(&envVarOutputFileName, "env-var-filename", flareDefaultEnvVarFileName, "The name of the file to which env vars will be written")
	flareCmd.Flags().StringVar(&zipOutputFileName, "zip-filename", flareDefaultZipFileName, "The file name of the output zip archive")
	rootCmd.AddCommand(flareCmd)
}
