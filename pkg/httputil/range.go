package httputil

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	ErrBadRange           = errors.New("invalid range")
	ErrUnsatisfiableRange = errors.New("unsatisfiable range")
)

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
	var r Range
	if !strings.HasPrefix(spec, "bytes=") {
		return r, ErrBadRange
	}
	spec = strings.TrimPrefix(spec, "bytes=")
	if strings.Contains(spec, ",") {
		// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
		return r, ErrBadRange
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return r, ErrBadRange
	}
	fromString := strings.TrimSpace(parts[0])
	toString := strings.TrimSpace(parts[1])
	if fromString == "" && toString == "" {
		return r, ErrBadRange
	}
	// Suffix range: bytes=-SUFFIX
	if fromString == "" {
		suffixLen, err := strconv.ParseInt(toString, 10, 64)
		if err != nil {
			return r, ErrBadRange
		}
		if suffixLen == 0 {
			return r, ErrUnsatisfiableRange
		}
		if suffixLen < 0 {
			return r, ErrBadRange
		}
		if suffixLen > length {
			r.StartOffset = 0
		} else {
			r.StartOffset = length - suffixLen
		}
		r.EndOffset = length - 1
		return r, nil
	}
	// Open-ended range: bytes=START-
	if toString == "" {
		start, err := strconv.ParseInt(fromString, 10, 64)
		if err != nil || start < 0 {
			return r, ErrBadRange
		}
		if start > length-1 {
			return r, ErrUnsatisfiableRange
		}
		r.StartOffset = start
		r.EndOffset = length - 1
		return r, nil
	}
	// Range: bytes=START-END
	start, err := strconv.ParseInt(fromString, 10, 64)
	if err != nil || start < 0 {
		return r, ErrBadRange
	}
	end, err := strconv.ParseInt(toString, 10, 64)
	if err != nil || end < 0 {
		return r, ErrBadRange
	}
	if start > end {
		return r, ErrBadRange
	}
	if start > length-1 {
		return r, ErrUnsatisfiableRange
	}
	if end > length-1 {
		end = length - 1
	}
	r.StartOffset = start
	r.EndOffset = end
	return r, nil
}
