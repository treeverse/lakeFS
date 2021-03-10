package crypt_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/pkg/auth/crypt"
)

func TestSecretStore_Encrypt(t *testing.T) {
	cases := []struct {
		Secret string
		Data   []byte
	}{
		{"some secret", []byte("test string")},
		{"some other secret with a different length", []byte("another test string")},
		{"", []byte("something else, empty secret")},
		{"1", []byte("short secret this time")},
		{"some secret", []byte("")},
	}

	for i, cas := range cases {
		t.Run(fmt.Sprintf("encrypt_%d", i), func(t *testing.T) {
			aes := crypt.NewSecretStore([]byte(cas.Secret))
			encrypted, err := aes.Encrypt(cas.Data)
			if err != nil {
				t.Fatal(err)
			}

			decrypted, err := aes.Decrypt(encrypted)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(cas.Data, decrypted) {
				t.Fatalf("expected decrypted data to equal original data %s, instead got %s", cas.Data, decrypted)
			}

		})
	}
}

func BenchmarkSecretStore_Encrypt(b *testing.B) {
	secret := "foo bar"
	data := []byte("some value, it doesn't really matter what")
	aes := crypt.NewSecretStore([]byte(secret))
	encrypted, err := aes.Encrypt(data)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = aes.Decrypt(encrypted)
	}
}
