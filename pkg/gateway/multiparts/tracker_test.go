package multiparts_test

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/thanhpk/randstr"
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

func BenchmarkMultipartsTrackerCreate(b *testing.B) {
	ctx := context.Background()
	b.Run("benchmarkCreateOp_DB", func(b *testing.B) { runDBBenchmark(b, ctx, benchmarkCreateOp) })
	b.Run("benchmarkCreateOp_KVmem", func(b *testing.B) { runKVMemBenchmark(b, ctx, benchmarkCreateOp) })
	b.Run("benchmarkCreateOp_KVpg", func(b *testing.B) { runKVPostgresBenchmark(b, ctx, benchmarkCreateOp) })
}

func BenchmarkMultipartsTrackerGetSeq(b *testing.B) {
	ctx := context.Background()
	b.Run("benchmarkGetOpSeq_DB", func(b *testing.B) { runDBBenchmark(b, ctx, benchmarkGetOpSeq) })
	b.Run("benchmarkGetOpSeq_KVmem", func(b *testing.B) { runKVMemBenchmark(b, ctx, benchmarkGetOpSeq) })
	b.Run("benchmarkGetOpSeq_KVpg", func(b *testing.B) { runKVPostgresBenchmark(b, ctx, benchmarkGetOpSeq) })
}

func BenchmarkMultipartsTrackerGetRand(b *testing.B) {
	ctx := context.Background()
	b.Run("benchmarkGetOpRand_DB", func(b *testing.B) { runDBBenchmark(b, ctx, benchmarkGetOpRand) })
	b.Run("benchmarkGetOpRand_KVmem", func(b *testing.B) { runKVMemBenchmark(b, ctx, benchmarkGetOpRand) })
	b.Run("benchmarkGetOpRand_KVpg", func(b *testing.B) { runKVPostgresBenchmark(b, ctx, benchmarkGetOpRand) })
}

func BenchmarkMultipartsTrackerDelete(b *testing.B) {
	ctx := context.Background()
	b.Run("benchmarkDeleteOpSeq_DB", func(b *testing.B) { runDBBenchmark(b, ctx, benchmarkDeleteOpSeq) })
	b.Run("benchmarkDeleteOpSeq_KVmem", func(b *testing.B) { runKVMemBenchmark(b, ctx, benchmarkDeleteOpSeq) })
	b.Run("benchmarkDeleteOpSeq_KVpg", func(b *testing.B) { runKVPostgresBenchmark(b, ctx, benchmarkDeleteOpSeq) })
}

func BenchmarkMultipartsTrackerMix(b *testing.B) {
	ctx := context.Background()
	b.Run("benchmarkMixedOps_DB", func(b *testing.B) { runDBBenchmark(b, ctx, benchmarkMixedOps) })
	b.Run("benchmarkMixedOps_KVmem", func(b *testing.B) { runKVMemBenchmark(b, ctx, benchmarkMixedOps) })
	b.Run("benchmarkMixedOps_KVpg", func(b *testing.B) { runKVPostgresBenchmark(b, ctx, benchmarkMixedOps) })
}

type RunBenchmarkFunc func(b *testing.B, ctx context.Context, tracker multiparts.Tracker)

func runDBBenchmark(b *testing.B, ctx context.Context, bmFunc RunBenchmarkFunc) {
	bmFunc(b, ctx, testDBTracker(b))
}

func runKVMemBenchmark(b *testing.B, ctx context.Context, bmFunc RunBenchmarkFunc) {
	store := kvtest.MakeStoreByName("mem", "")(b, context.Background())
	defer store.Close()
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
	bmFunc(b, ctx, tracker)
}

func runKVPostgresBenchmark(b *testing.B, ctx context.Context, bmFunc RunBenchmarkFunc) {
	store := kvtest.MakeStoreByName("postgres", databaseURI)(b, context.Background())
	defer store.Close()
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
	bmFunc(b, ctx, tracker)
}

func benchmarkCreateOp(b *testing.B, ctx context.Context, tracker multiparts.Tracker) {
	keys := randomStrings(b.N)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		testutil.Must(b, createMpu(ctx, tracker, keys[n]))
	}
}

func createMpu(ctx context.Context, tracker multiparts.Tracker, key string) error {
	return tracker.Create(ctx, multiparts.MultipartUpload{
		UploadID:        key,
		Path:            "path/to/" + key,
		CreationDate:    time.Now().Round(time.Second),
		PhysicalAddress: "phy_" + key,
		Metadata:        nil,
		ContentType:     "test/data",
	})
}

func benchmarkGetOpSeq(b *testing.B, ctx context.Context, tracker multiparts.Tracker) {
	keys := createEntriesForTest(b, ctx, tracker, b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := tracker.Get(ctx, keys[n])
		testutil.Must(b, err)
	}
}

func benchmarkGetOpRand(b *testing.B, ctx context.Context, tracker multiparts.Tracker) {
	keys := createEntriesForTest(b, ctx, tracker, b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := tracker.Get(ctx, keys[rand.Intn(b.N)])
		testutil.Must(b, err)
	}
}

func benchmarkDeleteOpSeq(b *testing.B, ctx context.Context, tracker multiparts.Tracker) {
	keys := createEntriesForTest(b, ctx, tracker, b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := tracker.Delete(ctx, keys[n])
		testutil.Must(b, err)
	}
}

func benchmarkMixedOps(b *testing.B, ctx context.Context, tracker multiparts.Tracker) {
	keys := randomStrings(b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// Performing random Create (10%), Get(80%) and Delete(10%).
		// errors are allowed as it is possible an access is requested before a key
		// is created or after it is deleted, or a re-attempt to create a key
		key := keys[rand.Intn(b.N)]
		op := rand.Intn(10)
		switch op {
		case 0: // Create
			createMpu(ctx, tracker, key)
		case 1: // Delete
			tracker.Delete(ctx, key)
		default: // Get
			tracker.Get(ctx, key)
		}
	}

}

func createEntriesForTest(t testing.TB, ctx context.Context, tracker multiparts.Tracker, num int) []string {
	keys := randomStrings(num)

	for n := 0; n < num; n++ {
		err := tracker.Create(ctx, multiparts.MultipartUpload{
			UploadID:        keys[n],
			Path:            "path/to/" + keys[n],
			CreationDate:    time.Now().Round(time.Second),
			PhysicalAddress: "phy_" + keys[n],

			Metadata:    nil,
			ContentType: "test/data",
		})
		testutil.Must(t, err)
	}

	return keys
}

// TODO - identical to a function in pkg/graveler/sstable/writer_test.go
// refactor to a common "test_helpers" or alike
func randomStrings(n int) []string {
	var keys []string
	for i := 0; i < n; i++ {
		keys = append(keys, randstr.String(20, "abcdefghijklmnopqrstuvwyz0123456789"))
	}
	return keys
}
