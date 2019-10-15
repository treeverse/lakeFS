package ident

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Identifiable interface {
	Identity() []byte
}

func Hash(thing Identifiable) string {
	hashBytes := sha256.Sum256(thing.Identity())
	return fmt.Sprintf("%x", hashBytes)
}

// MultiHash combines several hashes into one
func MultiHash(inp ...string) string {
	h := sha256.New()
	for _, str := range inp {
		h.Write([]byte(str))
	}
	return hex.EncodeToString(h.Sum(nil))
}
