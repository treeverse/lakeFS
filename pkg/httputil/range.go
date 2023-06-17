package httputil

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrBadRange = errors.New("invalid range")
var ErrUnsatisfiableRange = errors.New("unsatisfiable range")

// Range represents an RFC 2616 HTTP Range
type Range struct {
	StartOffset int64
	EndOffset   int64
}

func (r Range) String() string {
	return fmt.Sprintf("start=%d, end=%d (total=%d)", r.StartOffset, r.EndOffset, r.EndOffset-r.StartOffset+1)
}

func (r Range) Size() int64 {
	return r.EndOffset - r.StartOffset + 1
}

// ParseRange parses an HTTP RFC 2616 Range header value and returns an Range object for the given object length
func ParseRange(spec string, length int64) (Range, error) {
	// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
	var r Range
	if !strings.HasPrefix(spec, "bytes=") {
		return r, ErrBadRange
	}
	spec = strings.TrimPrefix(spec, "bytes=")
	parts := strings.Split(spec, "-")
	const rangeParts = 2
	if len(parts) != rangeParts {
		return r, ErrBadRange
	}

	fromString := parts[0]
	toString := parts[1]
	if len(fromString) == 0 && len(toString) == 0 {
		return r, ErrBadRange
	}
	// negative only
	if len(fromString) == 0 {
		endOffset, err := strconv.ParseInt(toString, 10, 64) //nolint: gomnd
		if err != nil {
			return r, ErrBadRange
		}
		r.StartOffset = length - endOffset
		if length-endOffset < 0 {
			r.StartOffset = 0
		}
		r.EndOffset = length - 1
		return r, nil
	}
	// positive only
	if len(toString) == 0 {
		beginOffset, err := strconv.ParseInt(fromString, 10, 64) //nolint: gomnd
		if err != nil {
			return r, ErrBadRange
		} else if beginOffset > length-1 {
			return r, ErrUnsatisfiableRange
		}
		r.StartOffset = beginOffset
		r.EndOffset = length - 1
		return r, nil
	}
	// both set
	beginOffset, err := strconv.ParseInt(fromString, 10, 64) //nolint: gomnd
	if err != nil {
		return r, ErrBadRange
	}
	endOffset, err := strconv.ParseInt(toString, 10, 64) //nolint: gomnd
	if err != nil {
		return r, ErrBadRange
	}
	// if endOffset exceeds length return length : this is how it works in s3 (presto for example uses range with a huge endOffset regardless to the file size)
	if endOffset > length-1 {
		endOffset = length - 1
	}
	// if the beginning offset is after the size, it is unsatisfiable
	if beginOffset > length-1 || endOffset > length-1 {
		return r, ErrUnsatisfiableRange
	}
	r.StartOffset = beginOffset
	r.EndOffset = endOffset
	return r, nil
}
