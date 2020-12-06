package graveler_test

import (
	"errors"
	"testing"

	"github.com/treeverse/lakefs/graveler"
)

func TestRevParse(t *testing.T) {
	table := []struct {
		Name        string
		Input       string
		Expected    graveler.ParsedRev
		ExpectedErr error
	}{
		{
			Name:  "just_branch",
			Input: "master",
			Expected: graveler.ParsedRev{
				BaseRev:   "master",
				Modifiers: make([]graveler.RevModifier, 0),
			},
		},
		{
			Name:  "branch_one_caret",
			Input: "master^",
			Expected: graveler.ParsedRev{
				BaseRev: "master",
				Modifiers: []graveler.RevModifier{
					{
						Type:  graveler.RevModTypeCaret,
						Value: 1,
					},
				},
			},
		},
		{
			Name:  "branch_two_caret",
			Input: "master^^",
			Expected: graveler.ParsedRev{
				BaseRev: "master",
				Modifiers: []graveler.RevModifier{
					{
						Type:  graveler.RevModTypeCaret,
						Value: 1,
					},
					{
						Type:  graveler.RevModTypeCaret,
						Value: 1,
					},
				},
			},
		},
		{
			Name:  "branch_two_caret_one_qualified",
			Input: "master^2^",
			Expected: graveler.ParsedRev{
				BaseRev: "master",
				Modifiers: []graveler.RevModifier{
					{
						Type:  graveler.RevModTypeCaret,
						Value: 2,
					},
					{
						Type:  graveler.RevModTypeCaret,
						Value: 1,
					},
				},
			},
		},
		{
			Name:  "branch_tilde_caret_tilde",
			Input: "master~^~3",
			Expected: graveler.ParsedRev{
				BaseRev: "master",
				Modifiers: []graveler.RevModifier{
					{
						Type:  graveler.RevModTypeTilde,
						Value: 1,
					},
					{
						Type:  graveler.RevModTypeCaret,
						Value: 1,
					},
					{
						Type:  graveler.RevModTypeTilde,
						Value: 3,
					},
				},
			},
		},
		{
			Name:        "no_base",
			Input:       "^^^3",
			ExpectedErr: graveler.ErrInvalidRef,
		},
		{
			Name:        "non_numeric_qualifier",
			Input:       "master^a",
			ExpectedErr: graveler.ErrInvalidRef,
		},
	}

	for _, cas := range table {
		t.Run(cas.Name, func(t *testing.T) {
			got, err := graveler.RevParse(graveler.Ref(cas.Input))
			if cas.ExpectedErr != nil {
				if !errors.Is(err, cas.ExpectedErr) {
					t.Fatalf("expected error of type: %s, got %v", cas.ExpectedErr, err)
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got.BaseRev != cas.Expected.BaseRev {
				t.Fatalf("expected base rev: %s got %s", cas.Expected.BaseRev, got.BaseRev)
			}

			if len(got.Modifiers) != len(cas.Expected.Modifiers) {
				t.Fatalf("got wrong number of modifiers, expected %d got %d",
					len(cas.Expected.Modifiers), len(got.Modifiers))
			}

			for i, m := range got.Modifiers {
				if m.Type != cas.Expected.Modifiers[i].Type {
					t.Fatalf("unexpected modifier at index %d: expected type %d got %d",
						i, cas.Expected.Modifiers[i].Type, m.Type)
				}
				if m.Value != cas.Expected.Modifiers[i].Value {
					t.Fatalf("unexpected modifier at index %d: expected value %d got %d",
						i, cas.Expected.Modifiers[i].Value, m.Value)
				}
			}
		})
	}
}
