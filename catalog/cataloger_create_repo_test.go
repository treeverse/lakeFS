package catalog

import (
	"context"
	"errors"
	"testing"
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
			name:    "missing branch",
			args:    args{name: "repo3", bucket: "bucket3", branch: ""},
			wantErr: true,
			asErr:   ErrInvalidValue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.CreateRepo(ctx, tt.args.name, tt.args.bucket, tt.args.branch)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateRepo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.asErr != nil && !errors.As(err, &tt.asErr) {
				t.Errorf("CreateRepo() error = %v, expected as %v", err, tt.asErr)
				return
			}
		})
	}
}
