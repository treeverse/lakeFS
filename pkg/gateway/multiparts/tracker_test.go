package multiparts_test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/thanhpk/randstr"
	"github.com/treeverse/lakefs/pkg/gateway/multiparts"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	_ "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var makeTestStore = kvtest.MakeStoreByName("mem", kvparams.KV{})

func testDBTracker(t testing.TB) multiparts.Tracker {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI)
	return multiparts.NewDBTracker(conn)
}

func TestTracker_Get(t *testing.T) {
	store := kvtest.GetStore(context.Background(), t)
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
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
	store := kvtest.GetStore(context.Background(), t)
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
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
	store := kvtest.GetStore(context.Background(), t)
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
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
	runBenchmarkMultipartsTracker(b, benchmarkCreateOp)
}

func BenchmarkMultipartsTrackerGetSeq(b *testing.B) {
	runBenchmarkMultipartsTracker(b, benchmarkGetOpSeq)
}

func BenchmarkMultipartsTrackerGetRand(b *testing.B) {
	runBenchmarkMultipartsTracker(b, benchmarkGetOpRand)
}

func BenchmarkMultipartsTrackerDelete(b *testing.B) {
	runBenchmarkMultipartsTracker(b, benchmarkDeleteOpSeq)
}

func BenchmarkMultipartsTrackerMix(b *testing.B) {
	runBenchmarkMultipartsTracker(b, benchmarkMixedOps)
}

func BenchmarkMultipartsTrackerFlowConcurrent(b *testing.B) {
	numParts := []int{100, 1000, 10000}
	concurrency := []int{10, 20, 50, 100, 500, 1000}
	for _, routines := range concurrency {
		for _, parts := range numParts {
			testParamsStr := fmt.Sprintf("routines_%d_parts_%d_", routines, parts)
			runBenchmarkMultipartsTrackerWithDescription(b, benchmarkFullFlowConcurrent, testParamsStr, routines, parts)
		}
	}
}

func runBenchmarkMultipartsTracker(b *testing.B, _ RunBenchmarkFunc, _ ...int) {
	runBenchmarkMultipartsTrackerWithDescription(b, benchmarkMixedOps, "")
}

func runBenchmarkMultipartsTrackerWithDescription(b *testing.B, bmFunc RunBenchmarkFunc, description string, concurrencyParams ...int) {
	ctx := context.Background()
	b.Run(description+"db", func(b *testing.B) { runDBBenchmark(b, ctx, bmFunc, concurrencyParams...) })
	b.Run(description+"kv_mem", func(b *testing.B) { runKVMemBenchmark(b, ctx, bmFunc, concurrencyParams...) })
	b.Run(description+"kv_postgres", func(b *testing.B) { runKVPostgresBenchmark(b, ctx, bmFunc, concurrencyParams...) })
	b.Run(description+"kv_dynamodb", func(b *testing.B) { runKVDynamoDBBenchmark(b, ctx, bmFunc, concurrencyParams...) })
}

// Concurrency params index in array
const (
	ConcurrencyNumRoutines = iota
	ConcurrencyNumParts
	ConcurrencyMaxParams
)

type RunBenchmarkFunc func(b *testing.B, ctx context.Context, tracker multiparts.Tracker, concurrencyParams ...int)

func runDBBenchmark(b *testing.B, ctx context.Context, bmFunc RunBenchmarkFunc, concurrencyParams ...int) {
	b.Helper()
	bmFunc(b, ctx, testDBTracker(b), concurrencyParams...)
}

func runKVMemBenchmark(b *testing.B, ctx context.Context, bmFunc RunBenchmarkFunc, concurrencyParams ...int) {
	b.Helper()
	store := makeTestStore(b, context.Background())
	defer store.Close()
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
	bmFunc(b, ctx, tracker, concurrencyParams...)
}

func runKVPostgresBenchmark(b *testing.B, ctx context.Context, bmFunc RunBenchmarkFunc, concurrencyParams ...int) {
	b.Helper()
	store := kvtest.MakeStoreByName("postgres", kvparams.KV{Postgres: &kvparams.Postgres{ConnectionString: databaseURI}})(b, context.Background())
	defer store.Close()
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
	bmFunc(b, ctx, tracker, concurrencyParams...)
}

func runKVDynamoDBBenchmark(b *testing.B, ctx context.Context, bmFunc RunBenchmarkFunc, concurrencyParams ...int) {
	b.Helper()
	store := testutil.GetDynamoDBProd(ctx, b)
	tracker := multiparts.NewTracker(kv.StoreMessage{Store: store})
	bmFunc(b, ctx, tracker, concurrencyParams...)
}

func benchmarkCreateOp(b *testing.B, ctx context.Context, tracker multiparts.Tracker, _ ...int) {
	keys := generateRandomKeys(b.N)
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

func benchmarkGetOpSeq(b *testing.B, ctx context.Context, tracker multiparts.Tracker, _ ...int) {
	keys := createEntriesForTest(b, ctx, tracker, b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := tracker.Get(ctx, keys[n])
		testutil.Must(b, err)
	}
}

func benchmarkGetOpRand(b *testing.B, ctx context.Context, tracker multiparts.Tracker, _ ...int) {
	keys := createEntriesForTest(b, ctx, tracker, b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := tracker.Get(ctx, keys[rand.Intn(b.N)])
		testutil.Must(b, err)
	}
}

func benchmarkDeleteOpSeq(b *testing.B, ctx context.Context, tracker multiparts.Tracker, _ ...int) {
	keys := createEntriesForTest(b, ctx, tracker, b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := tracker.Delete(ctx, keys[n])
		testutil.Must(b, err)
	}
}

func benchmarkMixedOps(b *testing.B, ctx context.Context, tracker multiparts.Tracker, _ ...int) {
	keys := generateRandomKeys(b.N)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		// Performing random Create (10%), Get(80%) and Delete(10%).
		// errors are allowed as it is possible an access is requested before a key
		// is created or after it is deleted, or a re-attempt to create a key
		key := keys[rand.Intn(b.N)]
		op := rand.Intn(10)
		switch op {
		case 0: // Create
			_ = createMpu(ctx, tracker, key)
		case 1: // Delete
			_ = tracker.Delete(ctx, key)
		default: // Get
			_, _ = tracker.Get(ctx, key)
		}
	}
}

func benchmarkFullFlowConcurrent(b *testing.B, ctx context.Context, tracker multiparts.Tracker, concurrencyParams ...int) {
	if len(concurrencyParams) != ConcurrencyMaxParams {
		b.Fatal("Wrong number of concurrency params", len(concurrencyParams))
	}
	ch := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(concurrencyParams[ConcurrencyNumRoutines])

	for i := 0; i < concurrencyParams[ConcurrencyNumRoutines]; i++ {
		go func() {
			defer wg.Done()

			for range ch {
				runSingleTrackerFlow(b, ctx, tracker, concurrencyParams[ConcurrencyNumParts])
			}
		}()
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ch <- true
	}

	close(ch)
	wg.Wait()
}

func runSingleTrackerFlow(t testing.TB, ctx context.Context, tracker multiparts.Tracker, numParts int) {
	key := generateRandomKey(randomKeyLength)

	err := createMpu(ctx, tracker, key)
	testutil.Must(t, err)

	for i := 0; i < numParts; i++ {
		_, err = tracker.Get(ctx, key)
		testutil.Must(t, err)
	}

	err = tracker.Delete(ctx, key)
	testutil.Must(t, err)
}

func createEntriesForTest(t testing.TB, ctx context.Context, tracker multiparts.Tracker, num int) []string {
	keys := generateRandomKeys(num)

	for n := 0; n < num; n++ {
		err := createMpu(ctx, tracker, keys[n])
		testutil.Must(t, err)
	}

	return keys
}

const (
	randomKeyCharset = "abcdefghijklmnopqrstuvwyz0123456789"
	randomKeyLength  = 20
)

func generateRandomKeys(n int) []string {
	var keys []string
	for i := 0; i < n; i++ {
		keys = append(keys, generateRandomKey(randomKeyLength))
	}
	return keys
}

func generateRandomKey(len int) string {
	return randstr.String(len, randomKeyCharset)
}
