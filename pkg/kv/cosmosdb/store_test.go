package cosmosdb_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/cosmosdb"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

func TestCosmosDB(t *testing.T) {
	kvtest.DriverTest(t, cosmosdb.DriverName, kvparams.Config{})
}
