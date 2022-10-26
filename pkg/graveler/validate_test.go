package graveler

import (
	"errors"
	"testing"
)

func TestValidateTagID(t *testing.T) {
	tests := []struct {
		name    string
		tag     TagID
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
		{name: "single slash", tag: "more/tags", wantErr: ErrInvalidValue},
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
				t.Errorf("ValidateTagID() error = %v, wantErr %v (%v)", err, tt.wantErr, tt.name)
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

func TestValidateBranchID(t *testing.T) {
	tests := []struct {
		name     string
		branchID BranchID
		wantErr  error
	}{
		{name: "alpha dash", branchID: "valid-branch", wantErr: nil},
		{name: "alpha underscore", branchID: "valid_branch", wantErr: nil},
		{name: "alpha numeric", branchID: "valid123", wantErr: nil},
		{name: "capital alpha numeric", branchID: "Valid123", wantErr: nil},
		{name: "alpha underscore numeric", branchID: "valid_123", wantErr: nil},
		{name: "alpha dash numeric", branchID: "valid-123", wantErr: nil},
		{name: "alpha numeric", branchID: "123valid", wantErr: nil},
		{name: "char1", branchID: "invalid~char", wantErr: ErrInvalidValue},
		{name: "alpha dot numeric", branchID: "invalid1.0", wantErr: ErrInvalidValue},
		{name: "alpha two dots", branchID: "invalid..branch", wantErr: ErrInvalidValue},
		{name: "ends with dot", branchID: "invalid.", wantErr: ErrInvalidValue},
		{name: "alpha slash", branchID: "invalid/branch", wantErr: ErrInvalidValue},
		{name: "alpha double slash", branchID: "invalid//branch", wantErr: ErrInvalidValue},
		{name: "alpha question mark", branchID: "invalid?branch", wantErr: ErrInvalidValue},
		{name: "alpha at", branchID: "invalid@branch", wantErr: ErrInvalidValue},
		{name: "alpha column", branchID: "invalid:branch", wantErr: ErrInvalidValue},
		{name: "alpha backslash", branchID: "invalid\\branch", wantErr: ErrInvalidValue},
		{name: "alpha square brackets", branchID: "invalid[branch]", wantErr: ErrInvalidValue},
		{name: "alpha curly brackets", branchID: "invalid{branch}", wantErr: ErrInvalidValue},
	}
	for _, tb := range tests {
		t.Run(tb.name, func(t *testing.T) {
			err := ValidateBranchID(tb.branchID)
			if !errors.Is(err, tb.wantErr) {
				t.Errorf("ValidateBranchID() error = %v, wantErr %v", err, tb.wantErr)
			}
		})
	}
}
