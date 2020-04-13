package crypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"io"
)

type AesEncrypter struct {
	secret string
}

func NewEncrypter(secret string) *AesEncrypter {
	return &AesEncrypter{secret: secret}
}

func (a *AesEncrypter) keyFromSecret() ([]byte, error) {
	hasher := sha256.New()
	hasher.Write([]byte(a.secret))
	return hasher.Sum(nil), nil
}

func (a *AesEncrypter) encode(nonce, encrypted []byte) []byte {
	buf := make([]byte, 2+len(nonce)+len(encrypted))
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(nonce)))
	for i, b := range nonce {
		buf[i+2] = b
	}
	for i, b := range encrypted {
		buf[i+2+len(nonce)] = b
	}
	return buf
}

func (a *AesEncrypter) decode(encoded []byte) (data, nonce []byte) {
	nonceLen := binary.BigEndian.Uint16(encoded[0:2])
	nonce = encoded[2 : nonceLen+2]
	data = encoded[2+nonceLen:]
	return
}

func (a *AesEncrypter) Encrypt(data []byte) ([]byte, error) {
	key, err := a.keyFromSecret()
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	encrypted := gcm.Seal(nil, nonce, data, nil)
	return a.encode(nonce, encrypted), nil
}

func (a *AesEncrypter) Decrypt(data []byte) ([]byte, error) {
	key, err := a.keyFromSecret()
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	encrypted, nonce := a.decode(data)
	return gcm.Open(nil, nonce, encrypted, nil)
}
