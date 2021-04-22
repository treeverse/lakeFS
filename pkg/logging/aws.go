package logging

type AWSAdapter struct {
	Logger Logger
}

func (l *AWSAdapter) Log(vars ...interface{}) {
	l.Logger.Debug(vars...)
}
