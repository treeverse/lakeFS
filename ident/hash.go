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

func HashMulti(things []Identifiable) string {
	h := sha256.New()
	for _, thing := range things {
		h.Write(thing.Identity())
	}
	hashBytes := h.Sum(nil)
	encoded, _ := multihash.Encode(hashBytes, multihash.SHA2_256)
	return hex.EncodeToString(encoded)
}
