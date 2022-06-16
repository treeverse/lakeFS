package config

import (
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	DefaultLoggingFormat       = "text"
	DefaultLoggingLevel        = "INFO"
	DefaultLoggingOutput       = "-"
	DefaultLoggingFilesKeepKey = 100
	DefaultAuditLogLevel       = "DEBUG"
)

func setupLogger() {
	// set output format
	logging.SetOutputFormat(viper.GetString(LoggingFormatKey))

	// set outputs
	output := viper.GetStringSlice(LoggingOutputKey)
	maxSizeMB := viper.GetInt(LoggingFileMaxSizeMBKey)
	fileKeep := viper.GetInt(LoggingFilesKeepKey)
	logging.SetOutputs(output, maxSizeMB, fileKeep)

	// set level
	logging.SetLevel(viper.GetString(LoggingLevelKey))
}
