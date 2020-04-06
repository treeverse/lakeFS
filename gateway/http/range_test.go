package http_test

import (
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/gateway/http"
)

func TestParseHTTPRange(t *testing.T) {
	cases := []struct {
		Spec          string
		Length        int
		ExpectedError bool
		ExpectedStart int
		ExpectedEnd   int
	}{
		{"bytes=0-20", 50, false, 0, 20},
		{"bytes=0-20", 10, false, 0, 9},
		{"bytes=-20", 50, false, 30, 49},
		{"bytes=20-", 50, false, 20, 49},
		{"bytes=-20", 10, true, 0, 0},
		{"bytes=0-20", 20, false, 0, 19},
		{"bytes=0-19", 20, false, 0, 19},
		{"bytes=-0-19", 20, true, 0, 0},
		{"bytess=0-19", 20, true, 0, 0},
		{"0-19", 20, true, 0, 0},
		{"bytes=-", 20, true, 0, 0},
		{"bytes=0-foo", 20, true, 0, 0},
		{"bytes=foo-19", 20, true, 0, 0},
		{"bytes=21-", 20, true, 0, 0},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s_length_%d", c.Spec, c.Length), func(t *testing.T) {
			r, err := http.ParseHTTPRange(c.Spec, int64(c.Length))
			if err != nil && !c.ExpectedError {
				t.Fatalf("got unexpected error parsing: %v", err)
			}
			if err == nil && c.ExpectedError {
				t.Fatalf("expected error parsing spec, didnt get one")
			}
			if r.EndOffset != int64(c.ExpectedEnd) {
				t.Fatalf("expected end offset: %d, got %d", c.ExpectedEnd, r.EndOffset)
			}
			if r.StartOffset != int64(c.ExpectedStart) {
				t.Fatalf("expected start offset: %d, got %d", c.ExpectedStart, r.StartOffset)
			}
		})
	}
}
