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
	"go.uber.org/ratelimit"
)

var (
	testRepoCacheConfig = ref.CacheConfig{
		Size:   1000,
		Expiry: 20 * time.Millisecond,
	}

	testCommitCacheConfig = ref.CacheConfig{
		Size:   5000,
		Expiry: 20 * time.Millisecond,
	}
)

func testRefManager(t testing.TB) (graveler.RefManager, *kv.StoreMessage) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := &kv.StoreMessage{Store: kvStore}
	cfg := ref.ManagerConfig{
		Executor:              batch.NopExecutor(),
		KVStore:               storeMessage,
		KVStoreLimited:        kv.NewStoreLimiter(kvStore, ratelimit.NewUnlimited()),
		AddressProvider:       ident.NewHexAddressProvider(),
		RepositoryCacheConfig: testRepoCacheConfig,
		CommitCacheConfig:     testCommitCacheConfig,
	}
	return ref.NewRefManager(cfg), storeMessage
}

func testRefManagerWithAddressProvider(t testing.TB, addressProvider ident.AddressProvider) (graveler.RefManager, *kv.StoreMessage) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := &kv.StoreMessage{Store: kvStore}
	cfg := ref.ManagerConfig{
		Executor:              batch.NopExecutor(),
		KVStore:               storeMessage,
		AddressProvider:       addressProvider,
		RepositoryCacheConfig: testRepoCacheConfig,
		CommitCacheConfig:     testCommitCacheConfig,
	}
	return ref.NewRefManager(cfg), storeMessage
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
