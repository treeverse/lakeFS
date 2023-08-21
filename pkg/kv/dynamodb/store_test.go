package dynamodb_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
)

func TestDynamoKV(t *testing.T) {
	kvtest.DriverTest(t, dynamodb.DriverName, kvparams.Config{DynamoDB: testParams})
}
