package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/treeverse/lakefs/testutil"
)

func TestCataloger_CreateMultipartUpload(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// setup test data
	if err := c.CreateRepository(ctx, "repo1", "s3://bucket1", "master"); err != nil {
		t.Fatal("create repository for testing", err)
	}
	if err := c.CreateMultipartUpload(ctx, "repo1", "uploadX", "/pathX", "/fileX", time.Now()); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
		repository      string
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
			args:    args{repository: "repo1", uploadID: "upload1", path: "/path1", physicalAddress: "/file1", creationTime: time.Now()},
			wantErr: false,
		},
		{
			name:    "exists",
			args:    args{repository: "repo1", uploadID: "uploadX", path: "/pathX", physicalAddress: "/fileX", creationTime: time.Now()},
			wantErr: true,
		},
		{
			name:    "unknown repository",
			args:    args{repository: "repo2", uploadID: "upload1", path: "/path1", physicalAddress: "/file1", creationTime: time.Now()},
			wantErr: true,
		},
		{
			name:    "missing path",
			args:    args{repository: "repo1", uploadID: "upload1", path: "", physicalAddress: "/file1", creationTime: time.Now()},
			wantErr: true,
		},
		{
			name:    "missing physical address",
			args:    args{repository: "repo1", uploadID: "upload1", path: "/path1", physicalAddress: "", creationTime: time.Now()},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := c.CreateMultipartUpload(ctx, tt.args.repository, tt.args.uploadID, tt.args.path, tt.args.physicalAddress, tt.args.creationTime)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CreateMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			part, err := c.GetMultipartUpload(ctx, tt.args.repository, tt.args.uploadID)
			testutil.MustDo(t, "Get multipart upload we just created", err)
			if part.PhysicalAddress != tt.args.physicalAddress {
				t.Errorf("Multipart upload created physical address=%s, expected=%s", part.PhysicalAddress, tt.args.physicalAddress)
			}
			if part.Repository != tt.args.repository {
				t.Errorf("Multipart upload created repository=%s, expected=%s", part.Repository, tt.args.repository)
			}
			if part.UploadID != tt.args.uploadID {
				t.Errorf("Multipart upload created uploadID=%s, expected=%s", part.UploadID, tt.args.uploadID)
			}
		})
	}
}
