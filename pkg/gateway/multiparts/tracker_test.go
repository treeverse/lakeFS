package multiparts_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/gateway/multiparts"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func testDBTracker(t testing.TB) multiparts.Tracker {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI)
	return multiparts.NewDBTracker(conn)
}

func TestTracker_Get(t *testing.T) {
	store := kvtest.MakeStoreByName("mem", "")(t, context.Background())
	defer store.Close()
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
	testTrackerGet(t, tracker)
}

func TestDBTracker_Get(t *testing.T) {
	tracker := testDBTracker(t)
	testTrackerGet(t, tracker)
}

func testTrackerGet(t *testing.T, tracker multiparts.Tracker) {
	ctx := context.Background()
	creationTime := time.Now().UTC().Round(time.Second) // round in order to remove the monotonic clock
	// setup test data
	if err := tracker.Create(ctx, multiparts.MultipartUpload{
		UploadID:        "upload1",
		Path:            "/path1",
		CreationDate:    creationTime,
		PhysicalAddress: "/file1",
		ContentType:     "example/data",
	}); err != nil {
		t.Fatal("create multipart upload for testing", err)
	}

	type args struct {
		uploadID string
	}
	tests := []struct {
		name    string
		args    args
		want    *multiparts.MultipartUpload
		wantErr bool
	}{
		{
			name: "exists",
			args: args{uploadID: "upload1"},
			want: &multiparts.MultipartUpload{
				UploadID:        "upload1",
				Path:            "/path1",
				CreationDate:    creationTime,
				PhysicalAddress: "/file1",
				ContentType:     "example/data",
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
			if err == nil {
				got.CreationDate = got.CreationDate.UTC() // deepEqual workaround for time
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTracker_Delete(t *testing.T) {
	store := kvtest.MakeStoreByName("mem", "")(t, context.Background())
	defer store.Close()
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
	testTrackerDelete(t, tracker)
}

func TestDBTracker_Delete(t *testing.T) {
	tracker := testDBTracker(t)
	testTrackerDelete(t, tracker)
}

func testTrackerDelete(t *testing.T, tracker multiparts.Tracker) {
	t.Helper()
	ctx := context.Background()

	// setup test data
	if err := tracker.Create(ctx, multiparts.MultipartUpload{
		UploadID:        "uploadX",
		Path:            "/pathX",
		CreationDate:    time.Now(),
		PhysicalAddress: "/fileX",
		Metadata:        nil,
		ContentType:     "example/data",
	}); err != nil {
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
			if err := tracker.Delete(ctx, tt.args.uploadID); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTracker_Create(t *testing.T) {
	store := kvtest.MakeStoreByName("mem", "")(t, context.Background())
	defer store.Close()
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
	testTrackerCreate(t, tracker)
}

func TestDBTracker_Create(t *testing.T) {
	tracker := testDBTracker(t)
	testTrackerCreate(t, tracker)
}

func testTrackerCreate(t *testing.T, tracker multiparts.Tracker) {
	t.Helper()
	ctx := context.Background()

	// setup test data
	if err := tracker.Create(ctx, multiparts.MultipartUpload{
		UploadID:        "uploadX",
		Path:            "/pathX",
		CreationDate:    time.Now(),
		PhysicalAddress: "/fileX",
		Metadata:        nil,
		ContentType:     "example/data",
	}); err != nil {
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
			err := tracker.Create(ctx, multiparts.MultipartUpload{
				UploadID:        tt.args.uploadID,
				Path:            tt.args.path,
				CreationDate:    tt.args.creationTime,
				PhysicalAddress: tt.args.physicalAddress,
				ContentType:     "example/data",
			})
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
