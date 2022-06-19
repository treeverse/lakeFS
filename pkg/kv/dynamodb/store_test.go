package dynamodb_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestDynamoKV(t *testing.T) {
	kvtest.TestDriver(t, dynamodb.DriverName, dsn)
}
