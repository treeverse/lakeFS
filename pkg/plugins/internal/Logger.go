package internal

import (
	"github.com/hashicorp/go-hclog"
	"github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/pkg/logging"
)

type HClogger struct {
	hclog.Logger
	logger logging.Logger
}

func NewHClogger(hcl hclog.Logger, logger logging.Logger) HClogger {
	return HClogger{Logger: hcl, logger: logger}
}

func (l HClogger) Log(level hclog.Level, msg string, args ...interface{}) {
	l.logger.Log(logrus.Level(level), msg, args)
}

func (l HClogger) Trace(msg string, args ...interface{}) {
	l.logger.Trace(msg, args)
}

func (l HClogger) Debug(msg string, args ...interface{}) {
	l.logger.Debug(msg, args)
}

func (l HClogger) Info(msg string, args ...interface{}) {
	l.logger.Info(msg, args)
}

func (l HClogger) Warn(msg string, args ...interface{}) {
	l.logger.Warn(msg, args)
}

func (l HClogger) Error(msg string, args ...interface{}) {
	l.logger.Error(msg, args)
}

func (l HClogger) IsTrace() bool {
	return l.logger.IsTracing()
}

func (l HClogger) IsDebug() bool {
	return l.logger.IsDebugging()
}

func (l HClogger) IsInfo() bool {
	return l.logger.IsInfo()
}

func (l HClogger) IsWarn() bool {
	return l.logger.IsWarn()
}

func (l HClogger) IsError() bool {
	return l.logger.IsError()
}

func (l HClogger) SetLevel(level hclog.Level) {
	logging.SetLevel(string(level))
}
