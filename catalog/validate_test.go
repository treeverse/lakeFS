package catalog

import (
	"errors"
	"testing"

	"github.com/treeverse/lakefs/graveler"
)

func TestValidateTagID(t *testing.T) {
	tests := []struct {
		name    string
		tag     graveler.TagID
		wantErr error
	}{
		{name: "empty", tag: "", wantErr: ErrRequiredValue},
		{name: "tilda", tag: "~tag", wantErr: nil},
		{name: "valid1", tag: "tag", wantErr: nil},
		{name: "valid2", tag: "v1.0", wantErr: nil},
		{name: "ends with dot", tag: "tag.", wantErr: ErrInvalidValue},
		{name: "ends with dot", tag: "tag.", wantErr: ErrInvalidValue},
		{name: "ends with lock", tag: "tag.lock", wantErr: ErrInvalidValue},
		{name: "space", tag: "a tag", wantErr: ErrInvalidValue},
		{name: "invalid control", tag: "a\x01tag", wantErr: ErrInvalidValue},
		{name: "double slash", tag: "more//tags", wantErr: ErrInvalidValue},
		{name: "double dot", tag: "more..tags", wantErr: ErrInvalidValue},
		{name: "template", tag: "more@{tags}", wantErr: ErrInvalidValue},
		{name: "invalid value", tag: "@", wantErr: ErrInvalidValue},
		{name: "question mark", tag: "tag?", wantErr: ErrInvalidValue},
		{name: "column", tag: "tag:tag", wantErr: ErrInvalidValue},
		{name: "back slash", tag: "tag\\tag", wantErr: ErrInvalidValue},
		{name: "open square brackets", tag: "ta[g]", wantErr: ErrInvalidValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTagID(tt.tag)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("ValidateTagID() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTagID_Type(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("ValidateTagID should panic on invalid type")
		}
	}()
	_ = ValidateTagID("tag")
}
