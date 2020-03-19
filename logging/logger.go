package logging

import (
	"context"
	"sync"

	"github.com/sirupsen/logrus"
)

var (
	formatterInitOnce sync.Once
)

const (
	LogFieldsContextKey = "log_fields"
)

type Fields map[string]interface{}

type Logger interface {
	WithField(key string, value interface{}) Logger
	WithFields(fields Fields) Logger
	WithError(err error) Logger
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Warning(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
	Panic(args ...interface{})
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})
}

type logrusEntryWrapper struct {
	e *logrus.Entry
}

func (l *logrusEntryWrapper) WithField(key string, value interface{}) Logger {
	return &logrusEntryWrapper{l.e.WithField(key, value)}
}

func (l *logrusEntryWrapper) WithFields(fields Fields) Logger {
	return &logrusEntryWrapper{l.e.WithFields(logrus.Fields(fields))}
}

func (l *logrusEntryWrapper) WithError(err error) Logger {
	return &logrusEntryWrapper{l.e.WithError(err)}
}

func (l logrusEntryWrapper) Debug(args ...interface{}) {
	l.e.Debug(args...)
}

func (l logrusEntryWrapper) Info(args ...interface{}) {
	l.e.Info(args...)
}

func (l logrusEntryWrapper) Warn(args ...interface{}) {
	l.e.Warn(args...)
}

func (l logrusEntryWrapper) Warning(args ...interface{}) {
	l.e.Warning(args...)
}

func (l logrusEntryWrapper) Error(args ...interface{}) {
	l.e.Error(args...)
}

func (l logrusEntryWrapper) Fatal(args ...interface{}) {
	l.e.Fatal(args...)
}

func (l logrusEntryWrapper) Panic(args ...interface{}) {
	l.e.Panic(args...)
}

func (l *logrusEntryWrapper) Debugf(format string, args ...interface{}) {
	l.e.Debugf(format, args...)
}

func (l *logrusEntryWrapper) Infof(format string, args ...interface{}) {
	l.e.Infof(format, args...)
}

func (l *logrusEntryWrapper) Warnf(format string, args ...interface{}) {
	l.e.Warnf(format, args...)
}

func (l *logrusEntryWrapper) Warningf(format string, args ...interface{}) {
	l.e.Warningf(format, args...)
}

func (l *logrusEntryWrapper) Errorf(format string, args ...interface{}) {
	l.e.Errorf(format, args...)
}

func (l *logrusEntryWrapper) Fatalf(format string, args ...interface{}) {
	l.e.Fatalf(format, args...)
}

func (l *logrusEntryWrapper) Panicf(format string, args ...interface{}) {
	l.e.Panicf(format, args...)
}

type logrusCallerFormatter struct {
	f logrus.Formatter
}

func (lf logrusCallerFormatter) Format(e *logrus.Entry) ([]byte, error) {
	e.Caller = getCaller()
	return lf.f.Format(e)
}

func Default() Logger {
	// wrap formatter with our own formatter that overrides caller
	formatterInitOnce.Do(func() {
		logrus.StandardLogger().Formatter = logrusCallerFormatter{logrus.StandardLogger().Formatter}
	})
	return &logrusEntryWrapper{
		e: logrus.NewEntry(logrus.StandardLogger()),
	}
}

func FromContext(ctx context.Context) Logger {
	log := Default()
	fields := ctx.Value(LogFieldsContextKey)
	if fields != nil {
		loggerFields := fields.(Fields)
		return log.WithFields(loggerFields)
	}
	return log
}

func AddFields(ctx context.Context, fields Fields) context.Context {
	ctxFields := ctx.Value(LogFieldsContextKey)
	loggerFields := Fields{}
	if ctxFields != nil {
		loggerFields = ctxFields.(Fields)
	}
	for k, v := range fields {
		loggerFields[k] = v
	}
	return context.WithValue(ctx, LogFieldsContextKey, loggerFields)
}
