package auth

import (
	crand "crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

const AkiaAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567" // Amazon AKIA alphabet is weird.

func KeyGenerator(length int) string {
	var b strings.Builder

	for {
		bbuf := make([]byte, length) // one byte of randomness per character isn't really needed but it makes the implementation simpler
		_, err := crand.Read(bbuf)
		if err != nil {
			continue // let's retry until we have enough entropy
		}
		for i := 0; i < length; i++ {
			c := bbuf[i]
			b.WriteByte(AkiaAlphabet[int(c)%len(AkiaAlphabet)])
		}
		break
	}
	return b.String()
}

func Base64StringGenerator(bytes int) string {
	var ret string
	bbuf := make([]byte, bytes)
	for {
		_, err := crand.Read(bbuf)
		if err != nil {
			continue
		}
		ret = base64.StdEncoding.EncodeToString(bbuf)
		break
	}
	return ret
}

func HexStringGenerator(bytes int) string {
	var ret string
	buf := make([]byte, bytes)
	for {
		_, err := crand.Read(buf)
		if err != nil {
			continue
		}
		ret = strings.ToUpper(hex.EncodeToString(buf))
		break
	}
	return ret
}
