package serde

import (
	"time"
)

const (
	AWSTimestampFormat = "2006-01-02T15:04:05.000Z"
)

func Timestamp(ts time.Time) string {
	return ts.UTC().Format(AWSTimestampFormat)
}
