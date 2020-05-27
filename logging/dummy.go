package logging

import "context"

type DummyLogger struct{}

func (d DummyLogger) WithContext(ctx context.Context) Logger {
	return d
}

func (d DummyLogger) WithField(key string, value interface{}) Logger {
	return d
}

func (d DummyLogger) WithFields(fields Fields) Logger {
	return d
}

func (d DummyLogger) WithError(err error) Logger {
	return d
}

func (d DummyLogger) Trace(args ...interface{}) {

}

func (d DummyLogger) Debug(args ...interface{}) {

}

func (d DummyLogger) Info(args ...interface{}) {

}

func (d DummyLogger) Warn(args ...interface{}) {

}

func (d DummyLogger) Warning(args ...interface{}) {

}

func (d DummyLogger) Error(args ...interface{}) {

}

func (d DummyLogger) Fatal(args ...interface{}) {

}

func (d DummyLogger) Panic(args ...interface{}) {

}

func (d DummyLogger) Tracef(format string, args ...interface{}) {

}

func (d DummyLogger) Debugf(format string, args ...interface{}) {

}

func (d DummyLogger) Infof(format string, args ...interface{}) {

}

func (d DummyLogger) Warnf(format string, args ...interface{}) {

}

func (d DummyLogger) Warningf(format string, args ...interface{}) {

}

func (d DummyLogger) Errorf(format string, args ...interface{}) {

}

func (d DummyLogger) Fatalf(format string, args ...interface{}) {

}

func (d DummyLogger) Panicf(format string, args ...interface{}) {

}

func Dummy() Logger {
	return DummyLogger{}
}
