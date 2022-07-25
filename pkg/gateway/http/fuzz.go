package http

import "strings"

func FuzzParseRange(data []byte) int {
	if len(data) < 10 {
		return 0
	}
	length := int64(data[0])
	var b strings.Builder
	b.WriteString("bytes=")
	b.WriteString(string(data[1:]))
	_, _ = ParseRange(b.String(), length)
	return 1
}
