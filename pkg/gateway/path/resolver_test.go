package path

import (
	"reflect"
	"testing"
)

func TestResolvePath(t *testing.T) {
	type args struct {
		encodedPath string
	}
	tests := []struct {
		name    string
		args    args
		want    ResolvedPath
		wantErr bool
	}{
		{
			name: "branch with root",
			args: args{encodedPath: "/Branch-1"},
			want: ResolvedPath{Ref: "Branch-1", Path: "", WithPath: false},
		},
		{
			name: "branch without root",
			args: args{encodedPath: "Branch-1"},
			want: ResolvedPath{Ref: "Branch-1", Path: "", WithPath: false},
		},
		{
			name: "branch and path",
			args: args{encodedPath: "Branch-1/dir1/file1"},
			want: ResolvedPath{Ref: "Branch-1", Path: "dir1/file1", WithPath: true},
		},
		{
			name: "branch ends with delimiter",
			args: args{encodedPath: "Branch-1/"},
			want: ResolvedPath{Ref: "Branch-1", Path: "", WithPath: true},
		},
		{
			name: "branch with path ends with delimiter",
			args: args{encodedPath: "Branch-1/path2/"},
			want: ResolvedPath{Ref: "Branch-1", Path: "path2/", WithPath: true},
		},
		{
			name: "branch with path both start and end with delimiter",
			args: args{encodedPath: "/Branch-1/path2/"},
			want: ResolvedPath{Ref: "Branch-1", Path: "path2/", WithPath: true},
		},
		{
			name: "branch with path that start and end with delimiter",
			args: args{encodedPath: "Branch-1//path3/"},
			want: ResolvedPath{Ref: "Branch-1", Path: "/path3/", WithPath: true},
		},
		{
			name: "empty",
			args: args{},
			want: ResolvedPath{},
		},
		{
			name: "have some space",
			args: args{encodedPath: "BrAnCh99/ a dir / a file"},
			want: ResolvedPath{
				Ref:      "BrAnCh99",
				Path:     " a dir / a file",
				WithPath: true,
			},
		},
		{
			name:    "invalid branch",
			args:    args{encodedPath: "-invalid"},
			wantErr: true,
		},
		{
			name:    "invalid branch",
			args:    args{encodedPath: "-invalid"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolvePath(tt.args.encodedPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("ResolvePath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ResolvePath() got = %v, want %v", got, tt.want)
			}
		})
	}
}
