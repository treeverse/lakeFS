package logging

import (
	"github.com/aws/smithy-go/logging"
)

type AWSAdapter struct {
	Logger Logger
}

func (l *AWSAdapter) Logf(classification logging.Classification, format string, v ...interface{}) {
	if classification == logging.Warn {
		l.Logger.Warnf(format, v...)
	} else {
		l.Logger.Debugf(format, v...)
	}
}
