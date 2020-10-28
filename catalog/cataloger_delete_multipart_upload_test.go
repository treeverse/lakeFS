package catalog

import (
	"context"
	"testing"
	"time"
)

func TestCataloger_DeleteMultipartUpload(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// setup test data
	if _, err := c.CreateRepository(ctx, "repo1", "s3://bucket1", "master"); err != nil {
		t.Fatal("create repository for testing", err)
	}
	if err := c.CreateMultipartUpload(ctx, "repo1", "uploadX", "/pathX", "/fileX", time.Now()); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
		repository string
		uploadID   string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "delete exist", args: args{repository: "repo1", uploadID: "uploadX"}, wantErr: false},
		{name: "delete not exist", args: args{repository: "repo1", uploadID: "upload1"}, wantErr: true},
		{name: "no repository", args: args{repository: "repo9", uploadID: "upload9"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.DeleteMultipartUpload(ctx, tt.args.repository, tt.args.uploadID); (err != nil) != tt.wantErr {
				t.Errorf("DeleteMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
