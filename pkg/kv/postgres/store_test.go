package postgres_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
)

func TestPostgresKV(t *testing.T) {
	kvtest.TestDriver(t, postgres.DriverName, kvparams.KV{Postgres: &kvparams.Postgres{ConnectionString: databaseURI, ScanPageSize: 10}})
}
