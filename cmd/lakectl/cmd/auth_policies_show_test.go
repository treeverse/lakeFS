package cmd

import (
	"encoding/json"
	"testing"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

func TestToPrintStatement(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		resource     string
		wantResource string // expected JSON of the resource field
	}{
		{
			name:         "single resource wildcard",
			resource:     "*",
			wantResource: `"*"`,
		},
		{
			name:         "single resource ARN",
			resource:     "arn:lakefs:fs:::repository/myrepo/object/*",
			wantResource: `"arn:lakefs:fs:::repository/myrepo/object/*"`,
		},
		{
			name:         "multi resource JSON array string",
			resource:     `["arn:lakefs:fs:::repository/a/object/f1","arn:lakefs:fs:::repository/a/object/f2"]`,
			wantResource: `["arn:lakefs:fs:::repository/a/object/f1","arn:lakefs:fs:::repository/a/object/f2"]`,
		},
		{
			name:         "single-element JSON array string",
			resource:     `["arn:lakefs:fs:::repository/myrepo/object/*"]`,
			wantResource: `["arn:lakefs:fs:::repository/myrepo/object/*"]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := apigen.Statement{
				Action:   []string{"fs:ReadObject"},
				Effect:   "allow",
				Resource: tt.resource,
			}
			got := toPrintStatement(s)

			// Verify the Resource field marshals to the expected JSON.
			raw, err := json.Marshal(got.Resource)
			if err != nil {
				t.Fatalf("marshal resource: %v", err)
			}
			if string(raw) != tt.wantResource {
				t.Errorf("resource = %s, want %s", raw, tt.wantResource)
			}

			// Verify the other fields are preserved.
			if got.Effect != s.Effect {
				t.Errorf("effect = %q, want %q", got.Effect, s.Effect)
			}
			if len(got.Action) != len(s.Action) || got.Action[0] != s.Action[0] {
				t.Errorf("action = %v, want %v", got.Action, s.Action)
			}
		})
	}
}
