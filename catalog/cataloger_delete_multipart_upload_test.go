package catalog

import (
	"context"
	"testing"
	"time"
)

func TestCataloger_DeleteMultipartUpload(t *testing.T) {
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
		repo     string
		uploadID string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "delete exist", args: args{repo: "repo1", uploadID: "uploadX"}, wantErr: false},
		{name: "delete not exist", args: args{repo: "repo1", uploadID: "upload1"}, wantErr: true},
		{name: "no repo", args: args{repo: "repo9", uploadID: "upload9"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.DeleteMultipartUpload(ctx, tt.args.repo, tt.args.uploadID); (err != nil) != tt.wantErr {
				t.Errorf("DeleteMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
