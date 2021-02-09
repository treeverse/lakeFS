package catalog

import (
	"errors"
	"io/ioutil"
	"path"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAction_Validate(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantErr  error
	}{
		{name: "full", filename: "action_full.yaml", wantErr: nil},
		{name: "required", filename: "action_required.yaml", wantErr: nil},
		{name: "duplicate id", filename: "action_duplicate_id.yaml", wantErr: ErrInvalidAction},
		{name: "invalid id", filename: "action_invalid_id.yaml", wantErr: ErrInvalidAction},
		{name: "invalid hook type", filename: "action_invalid_type.yaml", wantErr: ErrInvalidAction},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actionData, err := ioutil.ReadFile(path.Join("testdata", tt.filename))
			if err != nil {
				t.Fatalf("Failed to load testdata %s, err=%s", tt.filename, err)
			}
			var act Action
			err = yaml.Unmarshal(actionData, &act)
			if err != nil {
				t.Fatalf("Unmarshal action err=%s", err)
			}
			err = act.Validate()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
