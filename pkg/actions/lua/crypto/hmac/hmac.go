package hmac

import (
	"crypto/hmac"
	"crypto/sha1" //#nosec
	"crypto/sha256"
	"hash"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State) {
	hmacOpen := func(l *lua.State) int {
		lua.NewLibrary(l, hmacLibrary)
		return 1
	}
	lua.Require(l, "crypto/hmac", hmacOpen, false)
	l.Pop(1)
}

var hmacLibrary = []lua.RegistryFunction{
	{Name: "sign_sha256", Function: signSHA256},
	{Name: "sign_sha1", Function: signSHA1},
}

func signSHA256(l *lua.State) int {
	return encode(l, sha256.New)
}

func signSHA1(l *lua.State) int {
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
