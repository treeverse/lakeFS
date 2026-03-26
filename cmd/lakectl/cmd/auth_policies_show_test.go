package cmd

import (
	"encoding/json"
	"reflect"
	"strings"
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

// TestStatementOutFieldsInSync verifies that statementOut declares the same json fields
// as apigen.Statement, so that if apigen.Statement gains a new field the test fails.
func TestStatementOutFieldsInSync(t *testing.T) {
	t.Parallel()
	jsonTags := func(t reflect.Type) map[string]bool {
		tags := make(map[string]bool, t.NumField())
		for i := range t.NumField() {
			name := strings.Split(t.Field(i).Tag.Get("json"), ",")[0]
			tags[name] = true
		}
		return tags
	}

	apigenTags := jsonTags(reflect.TypeOf(apigen.Statement{}))
	outTags := jsonTags(reflect.TypeOf(statementOut{}))

	for name := range apigenTags {
		if !outTags[name] {
			t.Errorf("field %q in apigen.Statement is missing from statementOut", name)
		}
	}
	for name := range outTags {
		if !apigenTags[name] {
			t.Errorf("field %q in statementOut has no counterpart in apigen.Statement", name)
		}
	}
}
