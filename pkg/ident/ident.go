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

// linter is angry because h can be an io.Writer, but we explicitly want a Hash here.
//
//nolint:interfacer
func marshalType(h hash.Hash, addressType AddressType) {
	_, _ = h.Write([]byte{byte(addressType)})
}

func MarshalBytes(h hash.Hash, v []byte) {
	marshalType(h, AddressTypeBytes)
	MarshalInt64(h, int64(len(v)))
	_, _ = h.Write(v)
}

func MarshalString(h hash.Hash, v string) {
	marshalType(h, AddressTypeString)
	MarshalInt64(h, int64(len(v)))
	_, _ = h.Write([]byte(v))
}

func MarshalInt64(h hash.Hash, v int64) {
	const int64Bytes = 8
	marshalType(h, AddressTypeInt64)
	_, _ = h.Write([]byte{int64Bytes})
	bytes := make([]byte, int64Bytes)
	binary.BigEndian.PutUint64(bytes, uint64(v))
	_, _ = h.Write(bytes)
}

func MarshalStringSlice(h hash.Hash, v []string) {
	marshalType(h, AddressTypeStringSlice)
	MarshalInt64(h, int64(len(v)))
	for _, item := range v {
		MarshalString(h, item)
	}
}

func MarshalStringMap(h hash.Hash, v map[string]string) {
	marshalType(h, AddressTypeStringMap)
	MarshalInt64(h, int64(len(v)))
	keys := make([]string, len(v))
	i := 0
	for k := range v {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	for _, k := range keys {
		MarshalString(h, k)
		MarshalString(h, v[k])
	}
}

func MarshalIdentifiable(h hash.Hash, v Identifiable) {
	marshalType(h, AddressTypeEmbeddedIdentifiable)
	MarshalBytes(h, v.Identity())
}

type AddressWriter struct {
	hash.Hash
}

func NewAddressWriter() *AddressWriter {
	return &AddressWriter{Hash: sha256.New()}
}

func (b *AddressWriter) MarshalBytes(v []byte) *AddressWriter {
	MarshalBytes(b, v)
	return b
}

func (b *AddressWriter) MarshalString(v string) *AddressWriter {
	MarshalString(b, v)
	return b
}

func (b *AddressWriter) MarshalStringOpt(v string) *AddressWriter {
	if len(v) > 0 {
		MarshalString(b, v)
	}
	return b
}

func (b *AddressWriter) MarshalInt64(v int64) *AddressWriter {
	MarshalInt64(b, v)
	return b
}

func (b *AddressWriter) MarshalStringSlice(v []string) *AddressWriter {
	MarshalStringSlice(b, v)
	return b
}

func (b *AddressWriter) MarshalStringMap(v map[string]string) *AddressWriter {
	MarshalStringMap(b, v)
	return b
}

func (b *AddressWriter) MarshalIdentifiable(v Identifiable) *AddressWriter {
	MarshalIdentifiable(b, v)
	return b
}

func (b *AddressWriter) Identity() []byte {
	return b.Sum(nil)
}
