package multiparts_test

import (
	"bytes"
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/gateway/multiparts"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	database, _ := testutil.GetDB(t, databaseURI)
	kvStore := kvtest.MakeStoreByName(postgres.DriverName, kvparams.KV{Type: postgres.DriverName, Postgres: &kvparams.Postgres{ConnectionString: databaseURI}})(t, ctx)
	defer kvStore.Close()
	dbTracker := multiparts.NewDBTracker(database)

	data := createMigrateTestData(t, ctx, dbTracker, 300)

	buf := bytes.Buffer{}
	err := multiparts.Migrate(ctx, database.Pool(), nil, &buf)
	require.NoError(t, err)

	testutil.MustDo(t, "Import file", kv.Import(ctx, &buf, kvStore))
	kvTracker := multiparts.NewTracker(kv.StoreMessage{Store: kvStore})
	for _, entry := range data {
		m, err := kvTracker.Get(ctx, entry.UploadID)
		require.NoError(t, err)
		require.Equal(t, *m, entry)
	}
}

func createMigrateTestData(t *testing.T, ctx context.Context, tracker multiparts.Tracker, size int) []multiparts.MultipartUpload {
	t.Helper()
	data := make([]multiparts.MultipartUpload, 0)
	for i := 0; i < size; i++ {
		m := multiparts.MultipartUpload{
			UploadID:        "test" + strconv.Itoa(i),
			Path:            "somePath" + strconv.Itoa(i),
			CreationDate:    randTime(),
			PhysicalAddress: "phys" + strconv.Itoa(i),
			Metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			ContentType: "content" + strconv.Itoa(i),
		}
		data = append(data, m)
		err := tracker.Create(ctx, m)
		testutil.MustDo(t, "Create entry", err)
	}
	return data
}

func randTime() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2070, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min
	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).UTC()
}
