package cosmosdb_test

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/cosmosdb"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

func TestCosmosDB(t *testing.T) {
	t.Skip("CosmosDB tests are flaky due to the emulator. If you plan on running those, make sure to assign at least 3CPUs and" +
		" 4GB of memory to the container running the emulator.")
	kvtest.DriverTest(t, cosmosdb.DriverName, kvparams.Config{CosmosDB: testParams})
}
