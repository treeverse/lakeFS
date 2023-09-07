package cosmosdb_test

import (
	"context"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/cosmosdb"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvtest"
)

func TestCosmosDB(t *testing.T) {
	t.Skip("CosmosDB tests are flaky due to the emulator. If you plan on running those, make sure to assign at least 3CPUs and" +
		" 4GB of memory to the container running the emulator.")
	kvtest.DriverTest(t, func(t testing.TB, ctx context.Context) kv.Store {
		t.Helper()
		store, err := kv.Open(ctx, kvparams.Config{CosmosDB: testParams, Type: cosmosdb.DriverName})
		if err != nil {
			t.Fatalf("failed to open kv '%s' store: %s", cosmosdb.DriverName, err)
		}
		t.Cleanup(store.Close)
		return store
	})
}
