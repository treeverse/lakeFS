package staging_test

import (
	"bytes"
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/staging"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	kvpg "github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const tombstoneMod = 7

type stagingTestRecord struct {
	stagingToken graveler.StagingToken
	records      []graveler.ValueRecord
}

func TestMigrate(t *testing.T) {
	ctx := context.Background()
	conn, _ := testutil.GetDB(t, databaseURI)
	store, err := kv.Open(context.Background(), kvpg.DriverName, kvparams.KV{Postgres: &kvparams.Postgres{ConnectionString: databaseURI}})
	testutil.MustDo(t, "Open KV Store", err)
	kvStore := kv.StoreMessage{Store: store}
	t.Cleanup(func() {
		_, err = conn.Pool().Exec(ctx, `TRUNCATE kv`)
		if err != nil {
			t.Fatalf("failed to delete KV table from postgres DB %s", err)
		}
		store.Close()
	})
	dbMgr := staging.NewDBManager(conn)

	data := createMigrateTestData(t, ctx, dbMgr, 300)

	buf := bytes.Buffer{}
	err = staging.Migrate(ctx, conn.Pool(), &buf)
	require.NoError(t, err)

	testutil.MustDo(t, "Import file", kv.Import(ctx, &buf, kvStore.Store))
	kvMgr := staging.NewManager(ctx, kvStore)
	for _, entry := range data {
		for _, record := range entry.records {
			r, err := kvMgr.Get(ctx, entry.stagingToken, record.Key)
			require.NoError(t, err)
			require.Equal(t, record.Value, r)
		}
	}
}

func genRandomBytes(t *testing.T, size int) (blk []byte) {
	t.Helper()
	blk = make([]byte, size)
	_, err := rand.Read(blk)
	testutil.MustDo(t, "Generate random bytes", err)
	return
}

func createMigrateTestData(t *testing.T, ctx context.Context, mgr graveler.StagingManager, size int) []stagingTestRecord {
	t.Helper()
	d := make([]stagingTestRecord, 0)
	for i := 0; i < size; i++ {
		tokenData := stagingTestRecord{
			stagingToken: graveler.StagingToken("test_token_" + strconv.Itoa(i)),
			records:      make([]graveler.ValueRecord, 0),
		}
		for j := 0; j < rand.Intn(i+1); j++ { // rand.Intn(i+1) ensures at least one staging token has no records
			key := genRandomBytes(t, rand.Intn(100)+10)
			val := &graveler.Value{
				Identity: genRandomBytes(t, rand.Intn(100)+10),
				Data:     genRandomBytes(t, rand.Intn(100)+50),
			}
			if i%tombstoneMod != 0 { // Create tombstone record every tombstoneMod records
				val = nil
			}
			tokenData.records = append(tokenData.records, graveler.ValueRecord{
				Key:   key,
				Value: val,
			})
			err := mgr.Set(ctx, tokenData.stagingToken, key, val, false)
			testutil.MustDo(t, "Create entry", err)
		}
		d = append(d, tokenData)
	}
	return d
}
