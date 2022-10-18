package dynamodb_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

func TestDynamoKV(t *testing.T) {
	kvtest.TestDriver(t, dynamodb.DriverName, kvparams.KV{DynamoDB: testParams})
}
