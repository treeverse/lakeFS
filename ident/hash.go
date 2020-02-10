package ident

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const (
	HashName      = "sha256"
	HashBytes     = 32
	HashBits      = 256
	HashHexLength = 64
)

type Identifiable interface {
	Identity() []byte
}

func Empty() string {
	hashBytes := sha256.Sum256([]byte{})
	return fmt.Sprintf("%x", hashBytes)
}

func Bytes(data []byte) string {
	hashBytes := sha256.Sum256(data)
	return fmt.Sprintf("%x", hashBytes)
}

func Hash(thing Identifiable) string {
	return Bytes(thing.Identity())
}

// MultiHash combines several hashes into one
func MultiHash(inp ...string) string {
	h := sha256.New()
	for _, str := range inp {
		h.Write([]byte(str))
	}
	return hex.EncodeToString(h.Sum(nil))
}
