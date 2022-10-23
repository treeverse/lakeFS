package ref_test

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

func testRefManager(t testing.TB) (graveler.RefManager, kv.StoreMessage) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := kv.StoreMessage{Store: kvStore}
	return ref.NewKVRefManager(batch.NopExecutor(), storeMessage, ident.NewHexAddressProvider(), ref.WithRepositoryCache(false)), storeMessage
}

func testRefManagerWithKVAndAddressProvider(t testing.TB, addressProvider ident.AddressProvider) (graveler.RefManager, kv.StoreMessage) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := kv.StoreMessage{Store: kvStore}
	return ref.NewKVRefManager(batch.NopExecutor(), storeMessage, addressProvider, ref.WithRepositoryCache(false)), storeMessage
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		// keep the log level calm
		logging.SetLevel("panic")
	}

	code := m.Run()
	os.Exit(code)
}
