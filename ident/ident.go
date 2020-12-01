package ident

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
)

type Identifiable interface {
	Identity() []byte
}

func ContentAddress(entity Identifiable) string {
	h := sha256.New()
	_, _ = h.Write(entity.Identity())
	return hex.EncodeToString(h.Sum(nil))
}

type Buffer struct {
	buf []byte
}

func NewBuffer() *Buffer {
	return &Buffer{
		buf: make([]byte, 0),
	}
}

func (b *Buffer) WriteBytes(v []byte) {
	b.WriteInt64(int64(len(v)))
	b.buf = append(b.buf, v...)
}

func (b *Buffer) WriteString(v string) {
	b.WriteInt64(int64(len(v)))
	b.buf = append(b.buf, []byte(v)...)
}

func (b *Buffer) WriteInt64(v int64) {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, 8)
	b.buf = append(b.buf, bytes...)
	binary.BigEndian.PutUint64(bytes, uint64(v))
	b.buf = append(b.buf, bytes...)
}

func (b *Buffer) WriteStringMap(v map[string]string) {
	b.WriteInt64(int64(len(v)))
	keys := make([]string, len(v))
	i := 0
	for k := range v {
		keys[i] = k
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString(v[k])
	}
}

func (b *Buffer) Identity() []byte {
	return b.buf
}

func (b *Buffer) Address() string {
	return ContentAddress(b)
}
