//go:build !windows

package logging

import (
	"log/syslog"
	"strings"

	"github.com/sirupsen/logrus"
	lSyslog "github.com/sirupsen/logrus/hooks/syslog"
)

func ContextUnavailableWithSysLogs(sysLogsLevel string) Logger {
	// wrap formatter with our own formatter that overrides caller
	formatterInitOnce.Do(func() {
		defaultLogger.SetReportCaller(true)
		defaultLogger.SetNoLock()
		defaultLogger.Formatter = logrusCallerFormatter{defaultLogger.Formatter}
	})
	syslogOnce.Do(func() {
		var hook *lSyslog.SyslogHook
		var err error
		switch strings.ToLower(sysLogsLevel) {
		// There's no syslog level for trace, using debug instead.
		case "trace", "debug":
			hook, err = lSyslog.NewSyslogHook("", "", syslog.LOG_DEBUG, "")
		case "info":
			hook, err = lSyslog.NewSyslogHook("", "", syslog.LOG_INFO, "")
		case "warn", "warning":
			hook, err = lSyslog.NewSyslogHook("", "", syslog.LOG_WARNING, "")
		case "error":
			hook, err = lSyslog.NewSyslogHook("", "", syslog.LOG_ERR, "")
		case "panic", "null", "none":
			hook, err = lSyslog.NewSyslogHook("", "", syslog.LOG_CRIT, "")
		}
		if err != nil {
			defaultLogger.WithError(err).Error("failed to set syslog hook")
		} else {
			defaultLogger.AddHook(hook)
		}
	})
	return &logrusEntryWrapper{
		e: logrus.NewEntry(defaultLogger),
	}
}
