package actions_test

import (
	"errors"
	"os"
	"path"
	"testing"

	"github.com/treeverse/lakefs/actions"
)

func TestAction_ReadAction(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantErr  error
	}{
		{name: "full", filename: "action_full.yaml", wantErr: nil},
		{name: "required", filename: "action_required.yaml", wantErr: nil},
		{name: "duplicate id", filename: "action_duplicate_id.yaml", wantErr: actions.ErrInvalidAction},
		{name: "invalid id", filename: "action_invalid_id.yaml", wantErr: actions.ErrInvalidAction},
		{name: "invalid hook type", filename: "action_invalid_type.yaml", wantErr: actions.ErrInvalidAction},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(path.Join("testdata", tt.filename))
			if err != nil {
				t.Fatalf("Failed to load testdata %s, err=%s", tt.filename, err)
			}
			defer func() { _ = f.Close() }()
			act, err := actions.ReadAction(f)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("ReadAction() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && act == nil {
				t.Error("ReadAction() no error, missing Action")
			}
		})
	}
}
