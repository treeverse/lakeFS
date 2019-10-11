package ident

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/multiformats/go-multihash"
)

type Identifiable interface {
	Identity() []byte
}

func Hash(thing Identifiable) string {
	hashBytes := sha256.New().Sum(thing.Identity())
	encoded, _ := multihash.Encode(hashBytes, multihash.SHA2_256)
	return hex.EncodeToString(encoded)
}

// MultiHash combines several hashes into one
func MultiHash(inp ...string) string {
	h := sha256.New()
	for _, str := range inp {
		h.Write([]byte(str))
	}
	encoded, _ := multihash.Encode(h.Sum(nil), multihash.SHA2_256)
	return hex.EncodeToString(encoded)
}
