package dynamodb_test

import (
	"testing"

	_ "github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestDynamoKV(t *testing.T) {
	kvtest.TestDriver(t, "dynamodb", dsn)
}
