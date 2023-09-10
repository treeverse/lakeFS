package postgres_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
	"github.com/treeverse/lakefs/pkg/testutil"
)

func TestPostgresKV(t *testing.T) {
	databaseURI, cleanup := runDBInstance(pool, testutil.UniqueKVTableName())
	t.Cleanup(cleanup)

	kvtest.DriverTest(t, func(t testing.TB, ctx context.Context) kv.Store {
		t.Helper()

		conn, err := pgx.Connect(ctx, databaseURI)
		if err != nil {
			t.Fatalf("Unable to connect to database: %v", err)
		}
		defer conn.Close(context.Background())

		// create a new schema per test
		schemaName := "test_schema" + testutil.UniqueName()
		_, err = conn.Exec(context.Background(), fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schemaName))
		if err != nil {
			t.Fatalf("Error creating schema: %v", err)
		}

		store, err := kv.Open(ctx, kvparams.Config{
			Type:     postgres.DriverName,
			Postgres: &kvparams.Postgres{ConnectionString: fmt.Sprintf("%s&search_path=%s", databaseURI, schemaName), ScanPageSize: kvtest.MaxPageSize},
		})
		if err != nil {
			t.Fatalf("failed to open kv '%s' store: %s", postgres.DriverName, err)
		}
		t.Cleanup(store.Close)
		return store
	})
}
