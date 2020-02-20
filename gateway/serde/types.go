package serde

import (
	"time"
)

const (
	AWSTimestampFormat = "2006-01-02T15:04:05.000Z"
)

func Timestamp(ts int64) string {
	t := time.Unix(ts, 0)
	return t.UTC().Format(AWSTimestampFormat)
}
