package postgres_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestPostgresKV(t *testing.T) {
	kvtest.TestDriver(t, "postgres", databaseURI)
}
