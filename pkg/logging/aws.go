package logging

import aws_logging "github.com/aws/smithy-go/logging"

// TODO(ariels): Wire these logs up to the S3 adapter.

type AWSAdapter struct {
	Logger Logger
}

func (l *AWSAdapter) logFunc(cls aws_logging.Classification) func(...interface{}) {
	switch cls {
	case aws_logging.Warn:
		return l.Warn
	case aws_logging.Debug:
		return l.Debug
	default:
		return l.Warn
	}
}

func (l *AWSAdapter) logFuncf(cls aws_logging.Classification) func(format string, vars ...interface{}) {
	switch cls {
	case aws_logging.Warn:
		return l.Warnf
	case aws_logging.Debug:
		return l.Debugf
	default:
		return l.Warnf
	}
}

func (l *AWSAdapter) Warn(vars ...interface{}) {
	l.Logger.Warn(vars...)
}

func (l *AWSAdapter) Debug(vars ...interface{}) {
	l.Logger.Debug(vars...)
}

func (l *AWSAdapter) Warnf(format string, vars ...interface{}) {
	l.Logger.Warnf(format, vars...)
}

func (l *AWSAdapter) Debugf(format string, vars ...interface{}) {
	l.Logger.Debugf(format, vars...)
}

func (l *AWSAdapter) Log(cls aws_logging.Classification, vars ...interface{}) {
	l.logFunc(cls)(vars...)
}

func (l *AWSAdapter) Logf(cls aws_logging.Classification, format string, vars ...interface{}) {
	l.logFuncf(cls)(format, vars...)
}
