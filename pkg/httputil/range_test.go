package httputil_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/pkg/httputil"
)

func TestParseRange(t *testing.T) {
	cases := []struct {
		Spec          string
		Length        int
		ExpectedError error
		ExpectedStart int
		ExpectedEnd   int
		ExpectedSize  int
	}{
		{"bytes=0-20", 50, nil, 0, 20, 21},
		{"bytes=0-20", 10, nil, 0, 9, 10},
		{"bytes=-20", 50, nil, 30, 49, 20},
		{"bytes=20-", 50, nil, 20, 49, 30},
		{"bytes=-20", 10, nil, 0, 9, 10},
		{"bytes=0-20", 20, nil, 0, 19, 20},
		{"bytes=0-19", 20, nil, 0, 19, 20},
		{"bytes=1-300", 20, nil, 1, 19, 19},
		{"bytes=-0-19", 20, httputil.ErrBadRange, 0, 0, 0},
		{"bytess=0-19", 20, httputil.ErrBadRange, 0, 0, 0},
		{"0-19", 20, httputil.ErrBadRange, 0, 0, 0},
		{"bytes=-", 20, httputil.ErrBadRange, 0, 0, 0},
		{"bytes=0-foo", 20, httputil.ErrBadRange, 0, 0, 0},
		{"bytes=foo-19", 20, httputil.ErrBadRange, 0, 0, 0},
		{"bytes=20-", 20, httputil.ErrUnsatisfiableRange, 0, 0, 0},
		{"bytes=21-", 20, httputil.ErrUnsatisfiableRange, 0, 0, 0},
		{"bytes=19-", 20, nil, 19, 19, 1},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s_length_%d", c.Spec, c.Length), func(t *testing.T) {
			r, err := httputil.ParseRange(c.Spec, int64(c.Length))
			if !errors.Is(err, c.ExpectedError) {
				t.Fatalf("epxected error: %v, got %v!", c.ExpectedError, err)
			}
			if err != nil {
				return
			}

			if r.EndOffset != int64(c.ExpectedEnd) {
				t.Fatalf("expected end offset: %d, got %d", c.ExpectedEnd, r.EndOffset)
			}
			if r.StartOffset != int64(c.ExpectedStart) {
				t.Fatalf("expected start offset: %d, got %d", c.ExpectedStart, r.StartOffset)
			}
			if r.Size() != int64(c.ExpectedSize) {
				t.Fatalf("expected size: %d, got %d", c.ExpectedSize, r.Size())
			}
		})
	}
}
