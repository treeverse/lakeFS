package cmd

import (
	"encoding/json"
	"testing"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

func TestStatementDocMarshalJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		resource     string
		wantResource string // expected JSON value of the resource field
	}{
		{
			name:         "wildcard string",
			resource:     "*",
			wantResource: `"*"`,
		},
		{
			name:         "single ARN string",
			resource:     "arn:lakefs:fs:::repository/myrepo/object/*",
			wantResource: `"arn:lakefs:fs:::repository/myrepo/object/*"`,
		},
		{
			name:         "multi-resource JSON array string",
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
			doc := StatementDoc{
				Statement: []apigen.Statement{{
					Action:   []string{"fs:ReadObject"},
					Effect:   "allow",
					Resource: tt.resource,
				}},
			}
			out, err := json.Marshal(doc)
			if err != nil {
				t.Fatalf("MarshalJSON: %v", err)
			}
			// Extract just the resource field from the marshaled output.
			var parsed struct {
				Statement []struct {
					Resource json.RawMessage `json:"resource"`
				} `json:"statement"`
			}
			if err := json.Unmarshal(out, &parsed); err != nil {
				t.Fatalf("unmarshal output: %v", err)
			}
			if got := string(parsed.Statement[0].Resource); got != tt.wantResource {
				t.Errorf("resource = %s, want %s", got, tt.wantResource)
			}
		})
	}
}
