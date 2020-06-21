package catalog

import (
	"reflect"
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
			want:   "#6kfQBz477AZCUw",
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
			args:    args{ref: "#6kfQBz477AZCUw"},
			want:    &Ref{Branch: "feature", CommitID: 10},
			wantErr: false,
		},
		{
			name:    "invalid commit",
			args:    args{ref: "#-"},
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
