package cmd

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/flare"
)

const (
	flareFilePath       = "%s/flare/%s/"
	flareConfigFileName = "lakefs-config.yaml"
	flareEnvVarFileName = "lakefs-env.txt"
	flareZipFileName    = "lakefs-flare.zip"
)

var (
	startLogDate       string
	parsedStartLogDate *time.Time = nil
	endLogDate         string
	parsedEndLogDate   *time.Time = nil
	packageContents    bool       = false
	includeLogs        bool       = true
	includeEnvVars     bool       = true
)

var flareCmd = &cobra.Command{
	Use:   "flare",
	Short: "collect configuration, environment variables, and logs for debugging and troubleshooting",
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		cfg := loadConfig()
		logFormat := flare.LogFormat(cfg.Logging.Format)
		if !flare.SupportedLogFormat(string(logFormat)) {
			printMsgAndExit(fmt.Sprintf("unsupported log file format: %s ", cfg.Logging.Format))
		}
		flr, err := flare.NewFlare(logFormat)
		if err != nil {
			printMsgAndExit("failed to create flare instance", err)
		}
		preflightValidations(cfg, flr)
		// we already do the validation in the preflight
		if startLogDate != "" {
			*parsedStartLogDate, _ = time.Parse(flr.LogDateLayout, startLogDate)
		}
		if endLogDate != "" {
			*parsedEndLogDate, _ = time.Parse(flr.LogDateLayout, endLogDate)
		}

		path, err := os.Getwd()
		if err != nil {
			printMsgAndExit("failed to get working directory", err)
		}
		flarePath := fmt.Sprintf(flareFilePath, path, timestamp)
		err = os.MkdirAll(flarePath, flare.DirPermissions)
		if err != nil {
			msg := fmt.Sprintf("failed to create flare directory at %s", flarePath)
			printMsgAndExit(msg, err)
		}

		flr.ProcessConfig(cfg, flarePath, flareConfigFileName)

		if includeLogs {
			outFileIdx := slices.IndexFunc(cfg.Logging.Output, func(e string) bool {
				return e != "" && e != "-" && e != "="
			})

			err = flr.ProcessLogFiles(
				cfg.Logging.Output[outFileIdx],
				flarePath,
				parsedStartLogDate,
				parsedEndLogDate,
			)
			if err != nil {
				printMsgAndExit("failed to process log file ", err)
			}
		}

		if includeEnvVars {
			err = flr.ProcessEnvVars(flarePath, flareEnvVarFileName)
			if err != nil {
				printMsgAndExit("failed to process env vars ", err)
			}
		}

		if packageContents {
			err = flr.ZipFolder(flarePath, flareZipFileName)
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

func preflightValidations(cfg *config.Config, flr *flare.Flare) {
	hasFileOutput := flr.HasLogFileOutput(cfg.Logging.Output)
	if !hasFileOutput {
		printMsgAndExit("lakefs isn't configured to output logs to a file. ")
	}

	err := validateAndParseDateFlags(startLogDate, flr.LogDateLayout)
	if err != nil {
		printMsgAndExit("failed parsing start date flag ", err)
	}
	err = validateAndParseDateFlags(endLogDate, flr.LogDateLayout)
	if err != nil {
		printMsgAndExit("failed parsing end date flag ", err)
	}
}

func validateAndParseDateFlags(dateFlag, dateLayout string) error {
	if dateFlag == "" {
		return nil
	}

	if _, err := time.Parse(dateLayout, dateFlag); err != nil {
		return err
	}
	return nil
}

//nolint:gochecknoinits
func init() {
	flareCmd.Flags().StringVarP(&startLogDate, "log-start-date", "s", "", "Start date of logs to include in the ISO 8601 format")
	flareCmd.Flags().StringVarP(&endLogDate, "log-end-date", "e", "", "End date of logs to include in the ISO 8601 format")
	flareCmd.Flags().BoolVarP(&packageContents, "package", "p", false, "Package generated artifacts into a .zip file. Default: false")
	flareCmd.Flags().BoolVar(&includeLogs, "include-logs", true, "Collect logs. Default: true")
	flareCmd.Flags().BoolVar(&includeEnvVars, "include-env-vars", true, "Collect environment variables. Default: true")
	rootCmd.AddCommand(flareCmd)
}
