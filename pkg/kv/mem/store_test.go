package mem_test

import (
	"context"
	"github.com/treeverse/lakefs/pkg/kv"
	"testing"

	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/mem"
)

func TestMemKV(t *testing.T) {
	kvtest.DriverTest(t, func(t testing.TB, ctx context.Context) kv.Store {
		t.Helper()
		store, err := kv.Open(ctx, kvparams.Config{
			Type: mem.DriverName,
		})
		if err != nil {
			t.Fatalf("failed to open kv '%s' store: %s", mem.DriverName, err)
		}
		t.Cleanup(store.Close)
		return store
	})
}
