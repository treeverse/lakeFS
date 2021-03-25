package crypt

import (
	"crypto/rand"
	"errors"
	"io"

	"golang.org/x/crypto/nacl/secretbox"
	"golang.org/x/crypto/scrypt"
)

const (
	KeySaltBytes   = 8
	KeySizeBytes   = 32
	NonceSizeBytes = 24
)

type SecretStore interface {
	SharedSecret() []byte
	Encrypt(data []byte) ([]byte, error)
	Decrypt(encrypted []byte) ([]byte, error)
}

type NaclSecretStore struct {
	secret []byte
}

var (
	ErrFailDecrypt = errors.New("could not decrypt value")
)

func NewSecretStore(secret []byte) *NaclSecretStore {
	return &NaclSecretStore{secret: secret}
}

func (a *NaclSecretStore) SharedSecret() []byte {
	return a.secret
}

func (a *NaclSecretStore) kdf(storedSalt []byte) (key [KeySizeBytes]byte, salt [KeySaltBytes]byte, err error) {
	if storedSalt != nil {
		copy(salt[:], storedSalt)
	} else if _, err = io.ReadFull(rand.Reader, salt[:]); err != nil {
		return
	}
	// scrypt's N, r & p, benchmarked to run at about 1ms, since it's in the critical path.
	// fair trade-off for a high throughput low latency system
	keySlice, err := scrypt.Key(a.secret, salt[:], 512, 8, 1, 32)
	if err != nil {
		return
	}
	copy(key[:], keySlice)
	return
}

func (a *NaclSecretStore) Encrypt(data []byte) ([]byte, error) {
	// generate a random nonce
	var nonce [NonceSizeBytes]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nil, err
	}

	// derive a key from the stored secret, generating a new random salt
	key, salt, err := a.kdf(nil)
	if err != nil {
		return nil, err
	}

	// use nonce and derived key to encrypt data
	encrypted := secretbox.Seal(nonce[:], data, &nonce, &key)

	// kdf salt (8) + nonce (24) + encrypted data (rest)
	return append(salt[:], encrypted...), nil
}

func (a *NaclSecretStore) Decrypt(encrypted []byte) ([]byte, error) {
	// extract salt
	var salt [KeySaltBytes]byte
	copy(salt[:], encrypted[:KeySaltBytes])

	// derive encryption key from salt and stored secret
	key, _, err := a.kdf(salt[:])
	if err != nil {
		return nil, err
	}

	// extract nonce
	var decryptNonce [NonceSizeBytes]byte
	copy(decryptNonce[:], encrypted[KeySaltBytes:KeySaltBytes+NonceSizeBytes])

	// decrypt  the rest
	decrypted, ok := secretbox.Open(nil, encrypted[KeySaltBytes+NonceSizeBytes:], &decryptNonce, &key)
	if !ok {
		return nil, ErrFailDecrypt
	}
	return decrypted, nil
}
