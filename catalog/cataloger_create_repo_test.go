package catalog

import (
	"context"
	"errors"
	"testing"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_CreateRepo(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	type args struct {
		name   string
		bucket string
		branch string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
		asErr   error
	}{
		{
			name:    "basic",
			args:    args{name: "repo1", bucket: "bucket1", branch: "master"},
			wantErr: false,
			asErr:   nil,
		},
		{
			name:    "invalid bucket",
			args:    args{name: "repo2", bucket: "b", branch: "master"},
			wantErr: true,
			asErr:   ErrInvalidValue,
		},
		{
			name:    "unknown branch",
			args:    args{name: "repo3", bucket: "bucket3", branch: ""},
			wantErr: true,
			asErr:   ErrInvalidValue,
		},
		{
			name:    "missing repo",
			args:    args{name: "", bucket: "bucket1", branch: "master"},
			wantErr: true,
			asErr:   ErrInvalidValue,
		},
		{
			name:    "missing bucket",
			args:    args{name: "repo1", bucket: "", branch: "master"},
			wantErr: true,
			asErr:   ErrInvalidValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.CreateRepository(ctx, tt.args.name, tt.args.bucket, tt.args.branch)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreateRepository() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.asErr != nil && !errors.As(err, &tt.asErr) {
				t.Fatalf("CreateRepository() error = %v, expected as %v", err, tt.asErr)
			}
			if err != nil {
				return
			}
			// get repository information and verify we got what we created
			rep, err := c.GetRepository(ctx, tt.args.name)
			testutil.MustDo(t, "Get repository for create repository verification", err)
			if rep.Name != tt.args.name {
				t.Errorf("Create repository got name = %s, expected = %s", rep.Name, tt.args.name)
			}
			if rep.DefaultBranch != tt.args.branch {
				t.Errorf("Create repository got branch = %s, expected = %s", rep.DefaultBranch, tt.args.branch)
			}
			if rep.StorageNamespace != tt.args.bucket {
				t.Errorf("Create repository got branch = %s, expected = %s", rep.StorageNamespace, tt.args.bucket)
			}
		})
	}
}
