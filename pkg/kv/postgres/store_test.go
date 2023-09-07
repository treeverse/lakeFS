package postgres_test

import (
	"context"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/testutil"
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
)

func TestPostgresKV(t *testing.T) {
	kvtest.DriverTest(t, func(t testing.TB, ctx context.Context) kv.Store {
		t.Helper()
		databaseURI, cleanup := runDBInstance(pool, testutil.UniqueKVTableName())

		store, err := kv.Open(ctx, kvparams.Config{
			Type:     postgres.DriverName,
			Postgres: &kvparams.Postgres{ConnectionString: databaseURI, ScanPageSize: 10},
		})
		if err != nil {
			t.Fatalf("failed to open kv '%s' store: %s", postgres.DriverName, err)
		}
		t.Cleanup(store.Close)
		t.Cleanup(cleanup)
		return store
	})
}
