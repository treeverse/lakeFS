package model

import (
	"sort"
)

func identFromStrings(strings ...string) []byte {
	buf := make([]byte, 0)
	for _, str := range strings {
		buf = append(buf, []byte(str)...)
	}
	return buf
}

func identMapToString(data map[string]string) string {
	keys := make([]string, len(data))
	i := 0
	for k := range data {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	buf := make([]byte, 0)
	for _, k := range keys {
		buf = append(buf, []byte(k)...)
		buf = append(buf, []byte(data[k])...)
	}
	return string(buf)
}
