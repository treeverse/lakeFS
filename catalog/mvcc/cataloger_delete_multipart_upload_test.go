package mvcc

import (
	"context"
	"testing"
	"time"
)

func TestCataloger_DeleteMultipartUpload(t *testing.T) {
	ctx := context.Background()
	c := testCataloger(t)

	// setup test data
	if err := c.CreateMultipartUpload(ctx, "", "uploadX", "/pathX", "/fileX", time.Now()); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
		uploadID string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "delete exist", args: args{uploadID: "uploadX"}, wantErr: false},
		{name: "delete not exist", args: args{uploadID: "upload1"}, wantErr: true},
		{name: "no id", args: args{uploadID: ""}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := c.DeleteMultipartUpload(ctx, "", tt.args.uploadID); (err != nil) != tt.wantErr {
				t.Errorf("DeleteMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
