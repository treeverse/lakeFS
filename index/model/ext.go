package model

import (
	"fmt"
	"sort"
	"strconv"
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
	for k, _ := range data {
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

func (m *Entry) Identity() []byte {
	return identFromStrings(
		m.Name,
		fmt.Sprintf("%v", m.Type),
		strconv.FormatUint(m.Mtime, 10),
		identMapToString(m.Metadata))
}

func (m *Blob) Identity() []byte {
	return identFromStrings(m.Blocks...)
}

func (m *Commit) Identity() []byte {
	return append(identFromStrings(
		m.Tree,
		m.Committer,
		m.Message,
		strconv.FormatInt(m.Timestamp, 10),
		identMapToString(m.Metadata),
	), identFromStrings(m.Parents...)...)
}

func (m *Object) Identity() []byte {
	return append(
		m.Blob.Identity(),
		identFromStrings(
			identMapToString(m.Metadata),
		)...,
	)
}
