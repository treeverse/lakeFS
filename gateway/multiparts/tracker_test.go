package multiparts

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/treeverse/lakefs/testutil"
)

func testTracker(t testing.TB) Tracker {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI)
	return NewTracker(conn)
}

func Tracker_Get(t *testing.T) {
	ctx := context.Background()
	tracker := testTracker(t)

	creationTime := time.Now().Round(time.Second) // round in order to remove the monotonic clock
	// setup test data
	if err := tracker.Create(ctx, "upload1", "/path1", "/file1", creationTime); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
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
			args: args{uploadID: "upload1"},
			want: &MultipartUpload{
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
			got, err := tracker.Get(ctx, tt.args.uploadID)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Tracker_Delete(t *testing.T) {
	ctx := context.Background()
	c := testTracker(t)

	// setup test data
	if err := c.Create(ctx, "uploadX", "/pathX", "/fileX", time.Now()); err != nil {
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
			if err := c.Delete(ctx, tt.args.uploadID); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Tracker_Create(t *testing.T) {
	ctx := context.Background()
	tracker := testTracker(t)

	// setup test data
	if err := tracker.Create(ctx, "uploadX", "/pathX", "/fileX", time.Now()); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
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
			args:    args{uploadID: "upload1", path: "/path1", physicalAddress: "/file1", creationTime: time.Now()},
			wantErr: false,
		},
		{
			name:    "exists",
			args:    args{uploadID: "uploadX", path: "/pathX", physicalAddress: "/fileX", creationTime: time.Now()},
			wantErr: true,
		},
		{
			name:    "missing upload id",
			args:    args{uploadID: "upload1", path: "/path1", physicalAddress: "/file1", creationTime: time.Now()},
			wantErr: true,
		},
		{
			name:    "missing path",
			args:    args{uploadID: "upload1", path: "", physicalAddress: "/file1", creationTime: time.Now()},
			wantErr: true,
		},
		{
			name:    "missing physical address",
			args:    args{uploadID: "upload1", path: "/path1", physicalAddress: "", creationTime: time.Now()},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tracker.Create(ctx, tt.args.uploadID, tt.args.path, tt.args.physicalAddress, tt.args.creationTime)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Create() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			part, err := tracker.Get(ctx, tt.args.uploadID)
			testutil.MustDo(t, "Get multipart upload we just created", err)
			if part.PhysicalAddress != tt.args.physicalAddress {
				t.Errorf("Multipart upload created physical address=%s, expected=%s", part.PhysicalAddress, tt.args.physicalAddress)
			}
			if part.UploadID != tt.args.uploadID {
				t.Errorf("Multipart upload created uploadID=%s, expected=%s", part.UploadID, tt.args.uploadID)
			}
		})
	}
}
