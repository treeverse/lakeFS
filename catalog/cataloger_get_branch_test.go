package catalog

import (
	"context"
	"reflect"
	"testing"
)

func TestCataloger_GetBranch(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// setup test data
	if err := c.CreateRepository(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repository for testing failed", err)
	}

	type args struct {
		repository string
		branch     string
	}
	tests := []struct {
		name    string
		args    args
		want    *Branch
		wantErr bool
	}{
		{
			name:    "existing",
			args:    args{repository: "repo1", branch: "master"},
			want:    &Branch{Repository: "repo1", Name: "master"},
			wantErr: false,
		},
		{
			name:    "just repository",
			args:    args{repository: "repo1", branch: "shujin"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "unknown repository",
			args:    args{repository: "repoX", branch: "master"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing repository",
			args:    args{repository: "", branch: "master"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "unknown branch",
			args:    args{repository: "repo1", branch: "nobranch"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "missing branch",
			args:    args{repository: "repo1", branch: ""},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetBranch(ctx, tt.args.repository, tt.args.branch)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBranch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetBranch() got = %v, want %v", got, tt.want)
			}
		})
	}
}
