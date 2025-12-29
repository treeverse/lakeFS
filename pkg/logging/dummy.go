package logging

import (
	"context"

	"github.com/sirupsen/logrus"
)

type DummyLogger struct{}

func (d DummyLogger) WithContext(ctx context.Context) Logger {
	return d
}

func (d DummyLogger) WithField(key string, value any) Logger {
	return d
}

func (d DummyLogger) WithFields(fields Fields) Logger {
	return d
}

func (d DummyLogger) WithError(err error) Logger {
	return d
}

func (d DummyLogger) Trace(args ...any) {

}

func (d DummyLogger) Debug(args ...any) {

}

func (d DummyLogger) Info(args ...any) {

}

func (d DummyLogger) Warn(args ...any) {

}

func (d DummyLogger) Warning(args ...any) {

}

func (d DummyLogger) Error(args ...any) {

}

func (d DummyLogger) Fatal(args ...any) {

}

func (d DummyLogger) Panic(args ...any) {

}

func (d DummyLogger) Log(level logrus.Level, args ...any) {

}

func (d DummyLogger) Tracef(format string, args ...any) {

}

func (d DummyLogger) Debugf(format string, args ...any) {

}

func (d DummyLogger) Infof(format string, args ...any) {

}

func (d DummyLogger) Warnf(format string, args ...any) {

}

func (d DummyLogger) Warningf(format string, args ...any) {

}

func (d DummyLogger) Errorf(format string, args ...any) {

}

func (d DummyLogger) Fatalf(format string, args ...any) {

}

func (d DummyLogger) Panicf(format string, args ...any) {

}
func (d DummyLogger) Logf(level logrus.Level, format string, args ...any) {

}
func (d DummyLogger) IsTracing() bool {
	return true
}

func (d DummyLogger) IsDebugging() bool {
	return true
}

func (d DummyLogger) IsInfo() bool {
	return true
}

func (d DummyLogger) IsError() bool {
	return true
}

func (d DummyLogger) IsWarn() bool {
	return true
}

func Dummy() Logger {
	return DummyLogger{}
}
