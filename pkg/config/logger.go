package config

import (
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/logging"
)

const (
	DefaultLoggingFormat = "text"
	DefaultLoggingLevel  = "INFO"
	DefaultLoggingOutput = "-"
)

func setupLogger() {
	// set output format
	logging.SetOutputFormat(viper.GetString(LoggingFormatKey))

	// set output
	logging.SetOutput(viper.GetString(LoggingOutputKey))

	// set level
	logging.SetLevel(viper.GetString(LoggingLevelKey))
}
