package postgres_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/postgres"
)

func TestPostgresKV(t *testing.T) {
	kvtest.TestDriver(t, postgres.DriverName, databaseURI)
}
