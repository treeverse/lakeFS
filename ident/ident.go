package ident

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"sort"
)

type Identifiable interface {
	Identity() []byte
}

type AddressProvider interface {
	ContentAddress(entity Identifiable) string
}

type HexAddressProvider struct{}

func NewHexAddressProvider() *HexAddressProvider {
	return &HexAddressProvider{}
}

func (*HexAddressProvider) ContentAddress(entity Identifiable) string {
	return hex.EncodeToString(entity.Identity())
}

// IsContentAddress check if addr is valid content address or partial content address
func IsContentAddress(addr string) bool {
	if len(addr) == 0 || len(addr) > sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(addr)
	return err == nil
}

type AddressType uint8

const (
	AddressTypeBytes AddressType = iota
	AddressTypeString
	AddressTypeInt64
	AddressTypeStringSlice
	AddressTypeStringMap
	AddressTypeEmbeddedIdentifiable
)

type AddressWriter struct {
	hash.Hash
}

func NewAddressWriter() *AddressWriter {
	return &AddressWriter{sha256.New()}
}

func WriterFromHash(h hash.Hash) *AddressWriter {
	return &AddressWriter{h}
}

func (b *AddressWriter) marshalType(addressType AddressType) {
	_, _ = b.Write([]byte{byte(addressType)})
}

func (b *AddressWriter) MarshalBytes(v []byte) *AddressWriter {
	b.marshalType(AddressTypeBytes)
	b.MarshalInt64(int64(len(v)))
	_, _ = b.Write(v)
	return b
}

func (b *AddressWriter) MarshalString(v string) *AddressWriter {
	b.marshalType(AddressTypeString)
	b.MarshalInt64(int64(len(v)))
	_, _ = b.Write([]byte(v))
	return b
}

func (b *AddressWriter) MarshalInt64(v int64) *AddressWriter {
	b.marshalType(AddressTypeInt64)
	_, _ = b.Write([]byte{8})
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(v))
	_, _ = b.Write(bytes)
	return b
}

func (b *AddressWriter) MarshalStringSlice(v []string) *AddressWriter {
	b.marshalType(AddressTypeStringSlice)
	b.MarshalInt64(int64(len(v)))
	for _, item := range v {
		b.MarshalString(item)
	}
	return b
}

func (b *AddressWriter) MarshalStringMap(v map[string]string) *AddressWriter {
	b.marshalType(AddressTypeStringMap)
	b.MarshalInt64(int64(len(v)))
	keys := make([]string, len(v))
	i := 0
	for k := range v {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.MarshalString(k)
		b.MarshalString(v[k])
	}
	return b
}

func (b *AddressWriter) MarshalIdentifiable(v Identifiable) *AddressWriter {
	b.marshalType(AddressTypeEmbeddedIdentifiable)
	b.MarshalBytes(v.Identity())
	return b
}

func (b *AddressWriter) Identity() []byte {
	return b.Sum(nil)
}
