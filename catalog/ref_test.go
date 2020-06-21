package catalog

import (
	"reflect"
	"sort"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestReferenceSotrable(t *testing.T) {
	var commits []string
	for i := 1; i < 30; i++ {
		reference := MakeReference("master", CommitID(i))
		commits = append(commits, reference)
	}

	sorted := make([]string, len(commits))
	copy(sorted, commits)
	sort.Strings(sorted)

	if !reflect.DeepEqual(commits, sorted) {
		t.Fatalf("Commints are not sortable %s, expected %s", spew.Sdump(commits), spew.Sdump(sorted))
	}
}

func TestReference_String(t *testing.T) {
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
			name:   "branch",
			fields: fields{Branch: "master", CommitID: 0},
			want:   "master",
		},
		{
			name:   "commit",
			fields: fields{Branch: "feature", CommitID: 10},
			want:   "#DeNR42ZXB7Q4aAsh77uvji",
		},
		{
			name:   "uncommitted",
			fields: fields{Branch: "feature", CommitID: -1},
			want:   "feature#",
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

func TestParseReference(t *testing.T) {
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
			name:    "branch",
			args:    args{ref: "main"},
			want:    &Ref{Branch: "main", CommitID: CommittedID},
			wantErr: false,
		},
		{
			name:    "commit",
			args:    args{ref: "#DeNR42ZXB7Q4aAsh77uvji"},
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
		{
			name:    "uncommitted branch",
			args:    args{ref: "master#"},
			want:    &Ref{Branch: "master", CommitID: UncommittedID},
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
