package local

import "github.com/treeverse/lakefs/pkg/logging"

type BadgerLogger struct {
	logging.Logger
}

func (l *BadgerLogger) Debugf(_ string, _ ...any) {
	// we want to silence badger's useless debug logging
}

func (l *BadgerLogger) Debug(_ ...any) {
	// we want to silence badger's useless debug logging
}

func (l *BadgerLogger) Infof(_ string, _ ...any) {
	// we want to silence badger's useless info logging
}

func (l *BadgerLogger) Info(_ ...any) {
	// we want to silence badger's useless info logging
}
