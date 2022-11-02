package ref_test

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	testRepoCacheConfig = &ref.CacheConfig{
		Size:   ref.DefaultRepositoryCacheSize,
		Expiry: 20 * time.Millisecond,
		Jitter: 0,
	}

	testCommitCacheConfig = &ref.CacheConfig{
		Size:   ref.DefaultCommitCacheSize,
		Expiry: 20 * time.Millisecond,
		Jitter: 0,
	}
)

func testRefManager(t testing.TB) (graveler.RefManager, *kv.StoreMessage) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := &kv.StoreMessage{Store: kvStore}
	cfg := ref.ManagerConfig{
		Executor:          batch.NopExecutor(),
		KvStore:           storeMessage,
		AddressProvider:   ident.NewHexAddressProvider(),
		RepoCacheConfig:   testRepoCacheConfig,
		CommitCacheConfig: testCommitCacheConfig,
	}
	return ref.NewKVRefManager(cfg), storeMessage
}

func testRefManagerWithKVAndAddressProvider(t testing.TB, addressProvider ident.AddressProvider) (graveler.RefManager, *kv.StoreMessage) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := &kv.StoreMessage{Store: kvStore}
	cfg := ref.ManagerConfig{
		Executor:          batch.NopExecutor(),
		KvStore:           storeMessage,
		AddressProvider:   addressProvider,
		RepoCacheConfig:   testRepoCacheConfig,
		CommitCacheConfig: testCommitCacheConfig,
	}
	return ref.NewKVRefManager(cfg), storeMessage
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
