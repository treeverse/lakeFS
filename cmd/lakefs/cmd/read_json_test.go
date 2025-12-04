package cmd_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/go-test/deep"

	"github.com/treeverse/lakefs/cmd/lakefs/cmd"
)

type element struct {
	A int
	B string
}

type element2 struct {
	B string
	C int
}

func TestReadJSONOneType(t *testing.T) {
	cases := []struct {
		Name        string
		JSON        string
		Expected    element
		ExpectedErr error
	}{
		{
			Name:        "empty",
			JSON:        "",
			ExpectedErr: io.EOF,
		}, {
			Name:     "element",
			JSON:     `{"a": 3, "b": "three"}`,
			Expected: element{3, "three"},
		}, {
			Name:        "fail",
			JSON:        `[2,3,4]`,
			ExpectedErr: cmd.ErrNoMatch,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			r := strings.NewReader(tc.JSON)
			var got element
			err := cmd.ReadJSON(r, func(obj element) error {
				got = obj
				return nil
			})
			// Regular error comparison.
			if !errors.Is(err, tc.ExpectedErr) {
				t.Errorf("Got err %s expected %s", err, tc.ExpectedErr)
			}
			if diffs := deep.Equal(got, tc.Expected); diffs != nil {
				t.Error(diffs)
			}
		})
	}
}

func TestReadJSONTwoTypes(t *testing.T) {
	cases := []struct {
		Name        string
		JSON        string
		Expected    element
		Expected2   element2
		ExpectedErr error
	}{
		{
			Name:     "element",
			JSON:     `{"a": 3, "b": "three"}`,
			Expected: element{3, "three"},
		}, {
			Name:      "element2",
			JSON:      `{"b": "two", "c": 2}`,
			Expected2: element2{"two", 2},
		}, {
			Name:     "inOrder",
			JSON:     `{"b": "both"}`,
			Expected: element{0, "both"},
		}, {
			Name:        "neither",
			JSON:        `{"a": 2, "b": "both", "c": -2}`,
			ExpectedErr: cmd.ErrNoMatch,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			r := strings.NewReader(tc.JSON)
			var (
				got  element
				got2 element2
			)
			err := cmd.ReadJSON(r, func(obj element) error {
				got = obj
				return nil
			}, func(obj element2) error {
				got2 = obj
				return nil
			})
			// Regular error comparison.
			if !errors.Is(err, tc.ExpectedErr) {
				t.Errorf("Got err %s expected %s", err, tc.ExpectedErr)
			}
			if diffs := deep.Equal(got, tc.Expected); diffs != nil {
				t.Error("element: ", diffs)
			}
			if diffs := deep.Equal(got2, tc.Expected2); diffs != nil {
				t.Error("element2: ", diffs)
			}
		})
	}
}

func TestReadJSONCallbackError(t *testing.T) {
	r := strings.NewReader(`{"a": 1, "b": "one"}`)
	cbErr := errors.New("callback fail")
	err := cmd.ReadJSON(r, func(obj element) error {
		return cbErr
	})
	if !errors.Is(err, cbErr) {
		t.Fatalf("expected error %v, got %v", cbErr, err)
	}
}
