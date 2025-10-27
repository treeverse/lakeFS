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
		{Spec: "bytes=0-20", Length: 50, ExpectedStart: 0, ExpectedEnd: 20, ExpectedSize: 21},
		{Spec: "bytes=0-20", Length: 10, ExpectedStart: 0, ExpectedEnd: 9, ExpectedSize: 10},
		{Spec: "bytes=-20", Length: 50, ExpectedStart: 30, ExpectedEnd: 49, ExpectedSize: 20},
		{Spec: "bytes=20-", Length: 50, ExpectedStart: 20, ExpectedEnd: 49, ExpectedSize: 30},
		{Spec: "bytes=-20", Length: 10, ExpectedStart: 0, ExpectedEnd: 9, ExpectedSize: 10},
		{Spec: "bytes=0-20", Length: 20, ExpectedStart: 0, ExpectedEnd: 19, ExpectedSize: 20},
		{Spec: "bytes=0-19", Length: 20, ExpectedStart: 0, ExpectedEnd: 19, ExpectedSize: 20},
		{Spec: "bytes=1-300", Length: 20, ExpectedStart: 1, ExpectedEnd: 19, ExpectedSize: 19},
		{Spec: "bytes=19-", Length: 20, ExpectedStart: 19, ExpectedEnd: 19, ExpectedSize: 1},
		{Spec: "bytes=-0-19", Length: 20, ExpectedError: httputil.ErrBadRange},
		{Spec: "0-19", Length: 20, ExpectedError: httputil.ErrBadRange},
		{Spec: "bytes=-", Length: 20, ExpectedError: httputil.ErrBadRange},
		{Spec: "bytes=0-foo", Length: 20, ExpectedError: httputil.ErrBadRange},
		{Spec: "bytes=foo-19", Length: 20, ExpectedError: httputil.ErrBadRange},
		{Spec: "bytes=20-", Length: 20, ExpectedError: httputil.ErrUnsatisfiableRange},
		{Spec: "bytes=21-", Length: 20, ExpectedError: httputil.ErrUnsatisfiableRange},
		{Spec: "bytes=-0", Length: 20, ExpectedError: httputil.ErrUnsatisfiableRange},
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
