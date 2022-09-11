package local

import "github.com/treeverse/lakefs/pkg/logging"

type BadgerLogger struct {
	logging.Logger
}

func (l *BadgerLogger) Debugf(format string, args ...interface{}) {
	// we want to silence badger's useless debug logging
}
func (l BadgerLogger) Debug(args ...interface{}) {
	// we want to silence badger's useless debug logging
}
func (l *BadgerLogger) Infof(format string, args ...interface{}) {
	// we want to silence badger's useless info logging
}
func (l BadgerLogger) Info(args ...interface{}) {
	// we want to silence badger's useless info logging
}
