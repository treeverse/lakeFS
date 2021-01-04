package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	ModuleName           = "github.com/treeverse/lakefs"
	ProjectDirectoryName = "lakefs"

	DefaultLoggingFormat = "text"
	DefaultLoggingLevel  = "INFO"
	DefaultLoggingOutput = "-"
)

func setupLogger() {
	// add calling function/line numbers to log lines
	log.SetReportCaller(true)

	// trim calling function so it looks more readable
	logPrettyfier := func(frame *runtime.Frame) (function string, file string) {
		indexOfModule := strings.Index(strings.ToLower(frame.File), ProjectDirectoryName)
		if indexOfModule != -1 {
			file = frame.File[indexOfModule+len(ProjectDirectoryName):]
		} else {
			file = frame.File
		}
		file = fmt.Sprintf("%s:%d", strings.TrimPrefix(file, string(os.PathSeparator)), frame.Line)
		function = strings.TrimPrefix(frame.Function, fmt.Sprintf("%s%s", ModuleName, string(os.PathSeparator)))
		return
	}

	// set output format
	if strings.EqualFold(viper.GetString(LoggingFormatKey), "text") {
		log.SetFormatter(&log.TextFormatter{
			FullTimestamp:          true,
			DisableLevelTruncation: true,
			PadLevelText:           true,
			QuoteEmptyFields:       true,
			CallerPrettyfier:       logPrettyfier,
		})
	} else if strings.EqualFold(viper.GetString(LoggingFormatKey), "json") {
		log.SetFormatter(&log.JSONFormatter{
			CallerPrettyfier: logPrettyfier,
			PrettyPrint:      false,
		})
	}

	// set output
	if strings.EqualFold(viper.GetString(LoggingOutputKey), "-") {
		log.SetOutput(os.Stdout)
	} else {
		filename := viper.GetString("logging.output")
		handle, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0755)
		if err != nil {
			panic(fmt.Errorf("could not open log file: %w", err))
		}
		log.SetOutput(handle)
		// setup signal handler to reopen logfile on SIGHUP
		sigChannel := make(chan os.Signal, 1)
		signal.Notify(sigChannel, syscall.SIGHUP)
		go func() {
			for {
				<-sigChannel
				log.Info("SIGHUP received, rotating log file")
				log.SetOutput(ioutil.Discard)
				_ = handle.Close()
				handle, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0755)
				if err != nil {
					panic(fmt.Errorf("could not open log file: %w", err))
				}
				log.SetOutput(handle)
				log.Info("log file was rotated successfully")
			}
		}()
	}

	// set level
	switch strings.ToLower(viper.GetString(LoggingLevelKey)) {
	case "trace":
		log.SetLevel(log.TraceLevel)
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn", "warning":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "null", "none":
		log.SetOutput(ioutil.Discard)
	}
}
