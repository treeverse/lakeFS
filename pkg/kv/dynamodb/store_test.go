package dynamodb_test

import (
	"context"
	"testing"

	_ "github.com/treeverse/lakefs/pkg/kv/dynamodb"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestDynamoKV(t *testing.T) {
	kvtest.TestDriver(t, "dynamodb", databaseURI)
}

func TestTempTest(t *testing.T) {
	ms := kvtest.MakeStoreByName("dynamodb", "")
	ctx := context.Background()

	ms(t, ctx)
}
