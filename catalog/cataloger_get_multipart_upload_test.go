package catalog

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestCataloger_GetMultipartUpload(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	creationTime := time.Now().Round(time.Second) // round in order to remove the monotonic clock
	// setup test data
	if _, err := c.CreateRepository(ctx, "repo1", "s3://bucket1", "master"); err != nil {
		t.Fatal("create repository for testing failed", err)
	}
	if err := c.CreateMultipartUpload(ctx, "repo1", "upload1", "/path1", "/file1", creationTime); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
		repository string
		uploadID   string
	}
	tests := []struct {
		name    string
		args    args
		want    *MultipartUpload
		wantErr bool
	}{
		{
			name: "exists",
			args: args{repository: "repo1", uploadID: "upload1"},
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
			args:    args{repository: "repo1", uploadID: "upload2"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no repository",
			args:    args{repository: "repo2", uploadID: "upload1"},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.GetMultipartUpload(ctx, tt.args.repository, tt.args.uploadID)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetMultipartUpload() got = %v, want %v", got, tt.want)
			}
		})
	}
}
