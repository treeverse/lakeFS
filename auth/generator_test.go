package auth_test

import (
	"fmt"
	"testing"
	"versio-index/auth"
)

func TestKeyGenerator(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := auth.KeyGenerator(14)
		fmt.Printf("AKIAJ%sQ\n", k)
	}
}
