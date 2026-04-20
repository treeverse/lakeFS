package jwtidp

import (
	"errors"
	"fmt"

	"github.com/go-openapi/jsonpointer"
)

var ErrClaimType = errors.New("jwtidp: unexpected claim type")

// StringAt resolves the RFC 6901 JSON pointer ptr against claims and
// returns the string value, or ("", false, nil) when ptr is empty or
// resolves to nothing.
func StringAt(claims map[string]any, ptr string) (string, bool, error) {
	if ptr == "" {
		return "", false, nil
	}
	p, err := jsonpointer.New(ptr)
	if err != nil {
		return "", false, fmt.Errorf("invalid json pointer %q: %w", ptr, err)
	}
	v, _, err := p.Get(claims)
	// A missing claim is reported as not-found rather than an error
	// so callers can fall back to defaults.
	if err != nil || v == nil {
		return "", false, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", true, fmt.Errorf("%w: %q is %T, want string", ErrClaimType, ptr, v)
	}
	return s, true, nil
}
