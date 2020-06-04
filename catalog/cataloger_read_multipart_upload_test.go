package catalog

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestCataloger_ReadMultipartUpload(t *testing.T) {
	ctx := context.Background()
	c := setupCatalogerForTesting(t)

	creationTime := time.Now().Round(time.Second) // round in order to remove the monotonic clock
	// setup test data
	if err := c.CreateRepo(ctx, "repo1", "bucket1", "master"); err != nil {
		t.Fatal("create repo for testing failed", err)
	}
	if err := c.CreateMultipartUpload(ctx, "repo1", "upload1", "/path1", "/file1", creationTime); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
		repo     string
		uploadID string
	}
	tests := []struct {
		name    string
		args    args
		want    *MultipartUpload
		wantErr bool
	}{
		{
			name: "exists",
			args: args{repo: "repo1", uploadID: "upload1"},
			want: &MultipartUpload{
				Repository:      "repo1",
				UploadID:        "upload1",
				Path:            "/path1",
				CreationDate:    creationTime,
				PhysicalAddress: "/file1",
			},
			wantErr: false,
		},
		{
			name:    "not exists",
			args:    args{repo: "repo1", uploadID: "upload2"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no repo",
			args:    args{repo: "repo2", uploadID: "upload1"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.ReadMultipartUpload(ctx, tt.args.repo, tt.args.uploadID)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadMultipartUpload() got = %v, want %v", got, tt.want)
			}
		})
	}
}
