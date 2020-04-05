package http

import (
	"fmt"
	"golang.org/x/xerrors"
	"strconv"
	"strings"
)

var (
	ErrBadRange = xerrors.Errorf("unsatisfiable range")
)

// HttpRange represents an RFC 2616 HTTP Range
type HttpRange struct {
	StartOffset int64
	EndOffset   int64
}

func (r HttpRange) String() string {
	return fmt.Sprintf("start=%d, end=%d (total=%d)", r.StartOffset, r.EndOffset, r.EndOffset-r.StartOffset+1)
}

// ParseHTTPRange parses an HTTP RFC 2616 Range header value and returns an HttpRange object for the given object length
func ParseHTTPRange(spec string, length int64) (HttpRange, error) {
	// Amazon S3 doesn't support retrieving multiple ranges of data per GET request.
	var r HttpRange
	if !strings.HasPrefix(spec, "bytes=") {
		return r, ErrBadRange
	}
	spec = strings.TrimPrefix(spec, "bytes=")
	parts := strings.Split(spec, "-")
	if len(parts) != 2 {
		return r, ErrBadRange
	}

	fromString := parts[0]
	toString := parts[1]
	if len(fromString) == 0 && len(toString) == 0 {
		return r, ErrBadRange
	}
	// negative only
	if len(fromString) == 0 {
		endOffset, err := strconv.ParseInt(toString, 10, 64)
		if err != nil || endOffset > length {
			return r, ErrBadRange
		}
		r.StartOffset = length - endOffset
		r.EndOffset = length - 1
		return r, nil
	}
	// positive only
	if len(toString) == 0 {
		beginOffset, err := strconv.ParseInt(fromString, 10, 64)
		if err != nil || beginOffset > length-1 {
			return r, ErrBadRange
		}
		r.StartOffset = beginOffset
		r.EndOffset = length - 1
		return r, nil
	}
	// both set
	beginOffset, err := strconv.ParseInt(fromString, 10, 64)
	if err != nil {
		return r, ErrBadRange
	}
	endOffset, err := strconv.ParseInt(toString, 10, 64)
	if err != nil {
		return r, ErrBadRange
	}
	if beginOffset > length-1 || endOffset > length-1 {
		return r, ErrBadRange
	}
	r.StartOffset = beginOffset
	r.EndOffset = endOffset
	return r, nil
}
