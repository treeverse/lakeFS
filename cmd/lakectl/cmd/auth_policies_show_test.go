package cmd

import (
	"encoding/json"
	"testing"

	"github.com/treeverse/lakefs/pkg/api/apigen"
)

func TestStatementDocMarshalJSON(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		doc  StatementDoc
		want string
	}{
		{
			name: "single resource",
			doc: StatementDoc{Statement: []apigen.Statement{{
				Action:   []string{"fs:ReadObject"},
				Effect:   "allow",
				Resource: "*",
			}}},
			want: `{"statement":[{"action":["fs:ReadObject"],"effect":"allow","resource":"*"}]}`,
		},
		{
			name: "multi-resource JSON array string",
			doc: StatementDoc{Statement: []apigen.Statement{{
				Action:   []string{"fs:ReadObject"},
				Effect:   "allow",
				Resource: `["arn:lakefs:fs:::repository/a/object/f1","arn:lakefs:fs:::repository/a/object/f2"]`,
			}}},
			want: `{"statement":[{"action":["fs:ReadObject"],"effect":"allow","resource":["arn:lakefs:fs:::repository/a/object/f1","arn:lakefs:fs:::repository/a/object/f2"]}]}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, err := json.Marshal(tt.doc)
			if err != nil {
				t.Fatalf("MarshalJSON: %v", err)
			}
			if string(out) != tt.want {
				t.Errorf("got  %s\nwant %s", out, tt.want)
			}
		})
	}
}
