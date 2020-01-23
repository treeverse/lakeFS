package serde

import (
	"fmt"
	"time"
)

const (
	AWSTimestampFormat = "2006-01-02T15:04:05.000Z"

	// Last-Modified: <day-name>, <day> <month> <year> <hour>:<minute>:<second> GMT
	DateHeaderTimestampFormat = "Mon, 02 Jan 2006 15:04:05 GMT"
)

func Timestamp(ts int64) string {
	t := time.Unix(ts, 0)
	return t.UTC().Format(AWSTimestampFormat)
}

func HeaderTimestamp(ts int64) string {
	t := time.Unix(ts, 0)
	return t.UTC().Format(DateHeaderTimestampFormat)
}

func ETag(cksum string) string {
	return fmt.Sprintf("\"%s\"", cksum)
}
