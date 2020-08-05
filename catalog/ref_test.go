package catalog

import (
	"reflect"
	"strings"
	"testing"
)

func TestRef_String(t *testing.T) {
	type fields struct {
		Branch   string
		CommitID CommitID
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "uncommitted",
			fields: fields{Branch: "master", CommitID: UncommittedID},
			want:   "master",
		},
		{
			name:   "committed",
			fields: fields{Branch: "feature", CommitID: CommittedID},
			want:   "feature:HEAD",
		},
		{
			name:   "commit",
			fields: fields{Branch: "feature", CommitID: 10},
			want:   "~6kfQBz477AZCUw",
		},
		{
			name:   "empty",
			fields: fields{},
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := Ref{
				Branch:   tt.fields.Branch,
				CommitID: tt.fields.CommitID,
			}
			if got := r.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseRef(t *testing.T) {
	type args struct {
		ref string
	}
	tests := []struct {
		name    string
		args    args
		want    *Ref
		wantErr bool
	}{
		{
			name:    "uncommitted",
			args:    args{ref: "main"},
			want:    &Ref{Branch: "main", CommitID: UncommittedID},
			wantErr: false,
		},
		{
			name:    "committed",
			args:    args{ref: "main:HEAD"},
			want:    &Ref{Branch: "main", CommitID: CommittedID},
			wantErr: false,
		},
		{
			name:    "commit",
			args:    args{ref: "~6kfQBz477AZCUw"},
			want:    &Ref{Branch: "feature", CommitID: 10},
			wantErr: false,
		},
		{
			name:    "invalid commit",
			args:    args{ref: "~-"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty",
			args:    args{ref: ""},
			want:    &Ref{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRef(tt.args.ref)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRef() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseRef() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseInternalObjectRef(t *testing.T) {
	// Internal representation is _not_ user-visible, test just round-trip encode and parse.
	tests := []struct {
		obj  InternalObjectRef
		name string
	}{
		{name: "basic", obj: InternalObjectRef{BranchID: 1, MinCommit: 2, Path: "object"}},
		{name: "with path", obj: InternalObjectRef{BranchID: 1, MinCommit: 2, Path: "path/to/object"}},
		{name: "with spaces", obj: InternalObjectRef{BranchID: 1, MinCommit: 2, Path: "object with spaces"}},
		{name: "large IDs", obj: InternalObjectRef{BranchID: 0x7ffffffffffffffe, MinCommit: 0x7ffffffffffffffc, Path: ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := tt.obj.String()
			u, err := ParseInternalObjectRef(s)
			if err != nil {
				t.Fatalf("stringified %+v to %s but failed to parse it back: %s", tt, s, err)
			}
			if u.BranchID != tt.obj.BranchID {
				t.Errorf("branch id changed in round-trip %x -> %s -> %x", tt.obj.BranchID, s, u.BranchID)
			}
			if u.MinCommit != tt.obj.MinCommit {
				t.Errorf("min commit changed in round-trip %x -> %s -> %x", tt.obj.MinCommit, s, u.MinCommit)
			}
			if u.Path != tt.obj.Path {
				t.Errorf("path changed in round-trip %s -> %s -> %s", tt.obj.Path, s, u.Path)
			}
		})
	}
}

func TestParseInternalObjectRefStringFailures(t *testing.T) {
	// Internal representation is _not_ user-visible, test just round-trip encode and parse.
	tests := []struct {
		refString string
		name      string
		errMatch  string
	}{
		{name: "bad prefix", refString: "int:xpm:foo", errMatch: "unpack internal object format prefix"},
		{name: "bad base58 (colon)", refString: "int:pbm:2sCXVG8dD:Y", errMatch: "invalid base58 digit"},
		{name: "only 2 parts", refString: "int:pbm:ZdUu", errMatch: "expected 3 parts in internal object content"},
		{name: "bad branch id number (hex digit g)", refString: "int:pbm:vr3f9T8qYB3eTsfY", errMatch: "bad branchID"},
		{name: "bad min commit number (too large)", refString: "int:pbm:NfebWHSvY5YeEoVu2sAoohJ6UYq4HMna2LuwuSV45yD862jWJ", errMatch: "bad minCommit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := ParseInternalObjectRef(tt.refString)
			if err == nil {
				t.Fatalf("expected parsing \"%s\" to fail, got %+v", tt.refString, u)
			}
			if !strings.Contains(err.Error(), tt.errMatch) {
				t.Fatalf("expected failure message to include \"%s\", got %s", tt.errMatch, err)
			}
		})
	}
}
