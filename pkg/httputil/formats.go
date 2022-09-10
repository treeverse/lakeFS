package httputil

import (
	"strings"
	"time"
)

const (
	// DateHeaderTimestampFormat - Last-Modified: <day-name>, <day> <month> <year> <hour>:<minute>:<second> GMT
	DateHeaderTimestampFormat = "Mon, 02 Jan 2006 15:04:05 GMT"
)

func HeaderTimestamp(ts time.Time) string {
	return ts.UTC().Format(DateHeaderTimestampFormat)
}

func ETag(checksum string) string {
	if strings.HasPrefix(checksum, `"`) {
		return checksum
	}
	return `"` + checksum + `"`
}
