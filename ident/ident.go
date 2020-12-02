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

func (b *Buffer) MarshalBytes(v []byte) {
	b.MarshalInt64(int64(len(v)))
	b.buf = append(b.buf, v...)
}

func (b *Buffer) MarshalString(v string) {
	b.MarshalInt64(int64(len(v)))
	b.buf = append(b.buf, []byte(v)...)
}

func (b *Buffer) MarshalInt64(v int64) {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, 8)
	b.buf = append(b.buf, bytes...)
	binary.BigEndian.PutUint64(bytes, uint64(v))
	b.buf = append(b.buf, bytes...)
}

func (b *Buffer) MarshalStringList(v []string) {
	b.MarshalInt64(int64(len(v)))
	for _, item := range v {
		b.MarshalString(item)
	}
}

func (b *Buffer) MarshalStringMap(v map[string]string) {
	b.MarshalInt64(int64(len(v)))
	keys := make([]string, len(v))
	i := 0
	for k := range v {
		keys[i] = k
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.MarshalString(k)
		b.MarshalString(v[k])
	}
}

func (b *Buffer) MarshalIdentifiable(v Identifiable) {
	b.MarshalBytes(v.Identity())
}

func (b *Buffer) Identity() []byte {
	return b.buf
}
