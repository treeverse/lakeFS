package catalog

import (
	"context"
	"testing"
	"time"
)

func TestCataloger_CreateMultipartUpload(t *testing.T) {
	ctx := context.Background()
	c := setupCatalogerForTesting(t)

	// setup test data
	if err := c.CreateRepo(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repo for testing", err)
	}
	if err := c.CreateMultipartUpload(ctx, "repo1", "uploadX", "/pathX", "/fileX", time.Now()); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
		repo            string
		uploadID        string
		path            string
		physicalAddress string
		creationTime    time.Time
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "new",
			args:    args{repo: "repo1", uploadID: "upload1", path: "/path1", physicalAddress: "/file1", creationTime: time.Now()},
			wantErr: false,
		},
		{
			name:    "exists",
			args:    args{repo: "repo1", uploadID: "uploadX", path: "/pathX", physicalAddress: "/fileX", creationTime: time.Now()},
			wantErr: true,
		},
		{
			name:    "unknown repo",
			args:    args{repo: "repo2", uploadID: "upload1", path: "/path1", physicalAddress: "/file1", creationTime: time.Now()},
			wantErr: true,
		},
		{
			name:    "missing path",
			args:    args{repo: "repo1", uploadID: "upload1", path: "", physicalAddress: "/file1", creationTime: time.Now()},
			wantErr: true,
		},
		{
			name:    "missing physical address",
			args:    args{repo: "repo1", uploadID: "upload1", path: "/path1", physicalAddress: "", creationTime: time.Now()},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.CreateMultipartUpload(ctx, tt.args.repo, tt.args.uploadID, tt.args.path, tt.args.physicalAddress, tt.args.creationTime); (err != nil) != tt.wantErr {
				t.Errorf("CreateMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
