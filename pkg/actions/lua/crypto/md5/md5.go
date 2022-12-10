package hmac

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"hash"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State) {
	hmacOpen := func(l *lua.State) int {
		lua.NewLibrary(l, hmacLibrary)
		return 1
	}
	lua.Require(l, "goluago/crypto/hmac", hmacOpen, false)
	l.Pop(1)
}

var hmacLibrary = []lua.RegistryFunction{
	{"signsha256", signsha256},
	{"signsha1", signsha1},
}

func signsha256(l *lua.State) int {
	return encode(l, sha256.New)
}

func signsha1(l *lua.State) int {
	return encode(l, sha1.New)
}

func encode(l *lua.State, h func() hash.Hash) int {
	message := lua.CheckString(l, 1)
	key := lua.CheckString(l, 2)

	mac := hmac.New(h, []byte(key))
	mac.Write([]byte(message))
	l.PushString(string(mac.Sum(nil)))
	return 1
}
