package testutil

import (
	"io"
	"math"
	"math/rand"
	"strings"
	"unicode/utf8"
)

// RandomRune returns a random Unicode rune from rand, weighting at least
// num out of den runes to be ASCII.
func RandomRune(rand *rand.Rand, num, den int) rune {
	if rand.Intn(den) < num {
		return rune(rand.Intn(utf8.RuneSelf))
	}
	for {
		r := rune(rand.Intn(utf8.MaxRune))
		// Almost all chars are legal, so this usually
		// returns immediately.
		if utf8.ValidRune(r) {
			return r
		}
	}
}

// RandomString returns a random UTF-8 string of size or almost size bytes
// from rand.  It is weighted towards using many ASCII characters.
func RandomString(rand *rand.Rand, size int) string {
	sb := strings.Builder{}
	for sb.Len() <= size {
		sb.WriteRune(RandomRune(rand, 2, 5)) //nolint: mnd
	}
	ret := sb.String()
	_, lastRuneSize := utf8.DecodeLastRuneInString(ret)
	return ret[0 : len(ret)-lastRuneSize]
}

type randomReader struct {
	rand      *rand.Rand
	remaining int64
}

func (r *randomReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}
	n := len(p)
	if math.MaxInt >= r.remaining && n > int(r.remaining) {
		n = int(r.remaining)
	}
	// n still fits into int!
	r.rand.Read(p[:n])
	r.remaining -= int64(n)
	return n, nil
}

// NewRandomReader returns a reader that will return size bytes from rand.
func NewRandomReader(rand *rand.Rand, size int64) io.Reader {
	return &randomReader{rand: rand, remaining: size}
}
