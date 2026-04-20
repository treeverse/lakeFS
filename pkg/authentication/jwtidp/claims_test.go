package jwtidp_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/authentication/jwtidp"
)

// TestStringAt exercises the wrapper's unique behaviour around
// go-openapi/jsonpointer: empty pointer short-circuit, missing keys
// reported as not-found instead of error, non-string claims surfaced
// as a typed error, and invalid pointer syntax wrapped in our error.
// Nested / escaped-slash resolution is the library's job; one
// happy-path case is enough to prove the wiring.
func TestStringAt(t *testing.T) {
	claims := map[string]any{
		"sub": "user-123",
		"profile": map[string]any{
			"preferred_username": "alice",
		},
	}

	cases := []struct {
		name    string
		ptr     string
		want    string
		wantOk  bool
		wantErr bool
	}{
		{name: "happy path (nested)", ptr: "/profile/preferred_username", want: "alice", wantOk: true},
		{name: "empty pointer", ptr: "", want: "", wantOk: false},
		{name: "missing", ptr: "/missing", want: "", wantOk: false},
		{name: "wrong type", ptr: "/profile", wantErr: true},
		{name: "invalid pointer syntax", ptr: "no-leading-slash", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := jwtidp.StringAt(claims, tc.ptr)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantOk, ok)
			require.Equal(t, tc.want, got)
		})
	}
}
