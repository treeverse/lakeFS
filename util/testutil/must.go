package testutil

import "testing"

func Must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("error returned for operation: %v", err)
	}
}

func MustDo(t testing.TB, what string, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s, expected no error, got err=%s", what, err)
	}
}
