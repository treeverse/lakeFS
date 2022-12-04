package testutil

import (
	"os"
	"testing"
)

// WithEnvironmentVariable Sets an environment variable for the duration of the test,
//
//	restoring it to a previous value, if any, at teardown.
//
// Warning: Environment variables affect other goroutines that might be running at the same time -
//
//	so this function is *not* thread safe.
func WithEnvironmentVariable(t *testing.T, k, v string) {
	originalV, hasAny := os.LookupEnv(k)
	err := os.Setenv(k, v)
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Cleanup(func() {
		if hasAny {
			_ = os.Setenv(k, originalV)
		} else {
			_ = os.Unsetenv(k)
		}
	})
}
