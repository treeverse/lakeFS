package ref_test

import (
	"errors"
	"testing"

	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
)

func TestParseRef(t *testing.T) {
	table := []struct {
		Name        string
		Input       string
		Expected    graveler.RawRef
		ExpectedErr error
	}{
		{
			Name:  "just_branch",
			Input: "main",
			Expected: graveler.RawRef{
				BaseRef:   "main",
				Modifiers: make([]graveler.RefModifier, 0),
			},
		},
		{
			Name:  "branch_one_caret",
			Input: "main^",
			Expected: graveler.RawRef{
				BaseRef: "main",
				Modifiers: []graveler.RefModifier{
					{
						Type:  graveler.RefModTypeCaret,
						Value: 1,
					},
				},
			},
		},
		{
			Name:  "branch_one_dollar",
			Input: "main$",
			Expected: graveler.RawRef{
				BaseRef: "main",
				Modifiers: []graveler.RefModifier{
					{
						Type:  graveler.RefModTypeDollar,
						Value: 1,
					},
				},
			},
		},
		{
			Name:  "branch_one_at",
			Input: "main@",
			Expected: graveler.RawRef{
				BaseRef: "main",
				Modifiers: []graveler.RefModifier{
					{
						Type:  graveler.RefModTypeAt,
						Value: 1,
					},
				},
			},
		},
		{
			Name:        "branch_invalid_at",
			Input:       "main@1",
			ExpectedErr: graveler.ErrInvalidRef,
		},
		{
			Name:  "branch_two_caret",
			Input: "main^^",
			Expected: graveler.RawRef{
				BaseRef: "main",
				Modifiers: []graveler.RefModifier{
					{
						Type:  graveler.RefModTypeCaret,
						Value: 1,
					},
					{
						Type:  graveler.RefModTypeCaret,
						Value: 1,
					},
				},
			},
		},
		{
			Name:  "branch_two_caret_one_qualified",
			Input: "main^2^",
			Expected: graveler.RawRef{
				BaseRef: "main",
				Modifiers: []graveler.RefModifier{
					{
						Type:  graveler.RefModTypeCaret,
						Value: 2,
					},
					{
						Type:  graveler.RefModTypeCaret,
						Value: 1,
					},
				},
			},
		},
		{
			Name:  "branch_tilde_caret_tilde",
			Input: "main~^~3",
			Expected: graveler.RawRef{
				BaseRef: "main",
				Modifiers: []graveler.RefModifier{
					{
						Type:  graveler.RefModTypeTilde,
						Value: 1,
					},
					{
						Type:  graveler.RefModTypeCaret,
						Value: 1,
					},
					{
						Type:  graveler.RefModTypeTilde,
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
			Input:       "main^a",
			ExpectedErr: graveler.ErrInvalidRef,
		},
	}

	for _, cas := range table {
		t.Run(cas.Name, func(t *testing.T) {
			got, err := ref.ParseRef(graveler.Ref(cas.Input))
			if cas.ExpectedErr != nil {
				if !errors.Is(err, cas.ExpectedErr) {
					t.Fatalf("expected error of type: %s, got %v", cas.ExpectedErr, err)
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got.BaseRef != cas.Expected.BaseRef {
				t.Fatalf("expected base rev: %s got %s", cas.Expected.BaseRef, got.BaseRef)
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
