package db

import (
	"strings"
)

func Prefix(prefix string) string {
	v := strings.ReplaceAll(prefix, "%", "\\%")
	v = strings.ReplaceAll(v, "_", "\\_")
	return v + "%"
}
