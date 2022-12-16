package wildcard

import (
	"unicode/utf8"
)

// Match reports whether name matches the shell pattern.
// This is a strip down version of Go's `path.Match` https://pkg.go.dev/path#Match
func Match(pattern, name string) bool {
Pattern:
	for len(pattern) > 0 {
		var (
			star  bool
			chunk string
		)
		star, chunk, pattern = scanChunk(pattern)
		if star && chunk == "" {
			// Trailing * matches rest of string
			return true
		}
		// Look for match at current position.
		t, ok := matchChunk(chunk, name)
		// if we're the last chunk, make sure we've exhausted the name
		// otherwise we'll give a false result even if we could still match
		// using the star
		if ok && (len(t) == 0 || len(pattern) > 0) {
			name = t
			continue
		}
		if star {
			// Look for match skipping i+1 bytes.
			for i := 0; i < len(name); i++ {
				t, ok := matchChunk(chunk, name[i+1:])
				if ok {
					// if we're the last chunk, make sure we exhausted the name
					if len(pattern) == 0 && len(t) > 0 {
						continue
					}
					name = t
					continue Pattern
				}
			}
		}
		return false
	}
	return len(name) == 0
}

// scanChunk gets the next segment of pattern, which is a non-star string
// possibly preceded by a star.
func scanChunk(pattern string) (star bool, chunk, rest string) {
	for len(pattern) > 0 && pattern[0] == '*' {
		pattern = pattern[1:]
		star = true
	}
	for i := 1; i < len(pattern); i++ {
		if pattern[i] == '*' {
			return star, pattern[0:i], pattern[i:]
		}
	}
	return star, pattern, ""
}

// matchChunk checks whether chunk matches the beginning of s.
// If so, it returns the remainder of s (after the match).
// Chunk is all single-character operators: literals, char classes, and ?.
func matchChunk(chunk, s string) (rest string, ok bool) {
	for len(chunk) > 0 {
		if len(s) == 0 {
			return "", false
		}
		n := 1
		if chunk[0] == '?' {
			_, n = utf8.DecodeRuneInString(s)
		} else if chunk[0] != s[0] {
			return "", false
		}
		s = s[n:]
		chunk = chunk[1:]
	}
	return s, true
}
