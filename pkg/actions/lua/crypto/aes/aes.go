package aes

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	"github.com/Shopify/go-lua"
)

func Open(l *lua.State) {
	aesOpen := func(l *lua.State) int {
		lua.NewLibrary(l, aesLibrary)
		return 1
	}
	lua.Require(l, "crypto/aes", aesOpen, false)
	l.Pop(1)
}

var aesLibrary = []lua.RegistryFunction{
	{Name: "encryptCBC", Function: encryptCBC},
	{Name: "decryptCBC", Function: decryptCBC},
}

func encryptCBC(l *lua.State) int {
	key := []byte(lua.CheckString(l, 1))
	plaintext := []byte(lua.CheckString(l, 2))

	block, err := aes.NewCipher(key)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}

	content := PKCS5Padding(plaintext, aes.BlockSize, len(plaintext))

	ciphertext := make([]byte, aes.BlockSize+len(content))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}

	encrypter := cipher.NewCBCEncrypter(block, iv)
	encrypter.CryptBlocks(ciphertext[aes.BlockSize:], content)

	l.PushString(string(ciphertext))

	return 1
}

func decryptCBC(l *lua.State) int {
	key := []byte(lua.CheckString(l, 1))
	ciphertext := []byte(lua.CheckString(l, 2))

	block, err := aes.NewCipher(key)
	if err != nil {
		lua.Errorf(l, err.Error())
		panic("unreachable")
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	decrypter := cipher.NewCBCDecrypter(block, iv)
	decrypter.CryptBlocks(ciphertext, ciphertext)

	l.PushString(string(PKCS5UnPadding(ciphertext)))

	return 1
}

func PKCS5Padding(ciphertext []byte, blockSize int, after int) []byte {
	block := (blockSize - len(ciphertext)%blockSize)
	padding := bytes.Repeat([]byte{byte(block)}, block)
	return append(ciphertext, padding...)
}

func PKCS5UnPadding(src []byte) []byte {
	srcLength := len(src)
	paddingLength := int(src[srcLength-1])
	return src[:(srcLength - paddingLength)]
}
