package mvcc

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/treeverse/lakefs/catalog"
)

func TestCataloger_GetMultipartUpload(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	creationTime := time.Now().Round(time.Second) // round in order to remove the monotonic clock
	// setup test data
	if err := c.CreateMultipartUpload(ctx, "", "upload1", "/path1", "/file1", creationTime); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
		repository string
		uploadID   string
	}
	tests := []struct {
		name    string
		args    args
		want    *catalog.MultipartUpload
		wantErr bool
	}{
		{
			name: "exists",
			args: args{uploadID: "upload1"},
			want: &catalog.MultipartUpload{
				UploadID:        "upload1",
				Path:            "/path1",
				CreationDate:    creationTime,
				PhysicalAddress: "/file1",
			},
			wantErr: false,
		},
		{
			name:    "not exists",
			args:    args{uploadID: "upload2"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no upload id",
			args:    args{uploadID: ""},
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
