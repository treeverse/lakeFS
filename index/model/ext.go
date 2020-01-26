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

func (m *Entry) Identity() []byte {
	return identFromStrings(
		m.GetName(),
		m.GetAddress(),
		fmt.Sprintf("%v", m.Type))
}

func (m *Commit) Identity() []byte {
	return append(identFromStrings(
		m.GetTree(),
		m.GetCommitter(),
		m.GetMessage(),
		strconv.FormatInt(m.GetTimestamp(), 10),
		identMapToString(m.GetMetadata()),
	), identFromStrings(m.GetParents()...)...)
}

func (m *Object) Identity() []byte {
	addresses := make([]string, len(m.GetBlocks()))
	for i, block := range m.GetBlocks() {
		addresses[i] = block.GetAddress()
	}
	return append(
		identFromStrings(addresses...),
		identFromStrings(
			identMapToString(m.GetMetadata()),
		)...,
	)
}

func (m *MultipartUploadPart) Identity() []byte {
	addresses := make([]string, len(m.GetBlocks()))
	for i, block := range m.GetBlocks() {
		addresses[i] = block.GetAddress()
	}
	return identFromStrings(addresses...)
}
