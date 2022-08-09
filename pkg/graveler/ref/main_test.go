package ref_test

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/pkg/batch"
	"github.com/treeverse/lakefs/pkg/db"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/ref"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/testutil"
)

var (
	pool        *dockertest.Pool
	databaseURI string
)

type DBType struct {
	name       string
	refManager graveler.RefManager
}

const (
	typeDB = "DB"
	typeKV = "KV"
)

func testRefManagerWithDB(t testing.TB) (graveler.RefManager, db.Database) {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI, testutil.WithGetDBApplyDDL(true))
	return ref.NewPGRefManager(batch.NopExecutor(), conn, ident.NewHexAddressProvider()), conn
}

func testRefManagerWithKV(t testing.TB) (graveler.RefManager, kv.StoreMessage) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := kv.StoreMessage{Store: kvStore}
	return ref.NewKVRefManager(batch.NopExecutor(), storeMessage, ident.NewHexAddressProvider()), storeMessage
}

func testRefManager(t *testing.T) []DBType {
	t.Helper()
	dbRefManager, _ := testRefManagerWithDB(t)
	kvRefManager, _ := testRefManagerWithKV(t)

	tests := []DBType{
		{
			name:       typeDB,
			refManager: dbRefManager,
		},
		{
			name:       typeKV,
			refManager: kvRefManager,
		},
	}
	return tests
}

func testRefManagerWithDBAndAddressProvider(t testing.TB, addressProvider ident.AddressProvider) (graveler.RefManager, db.Database) {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI, testutil.WithGetDBApplyDDL(true))
	return ref.NewPGRefManager(batch.NopExecutor(), conn, addressProvider), conn
}

func testRefManagerWithKVAndAddressProvider(t testing.TB, addressProvider ident.AddressProvider) (graveler.RefManager, kv.StoreMessage) {
	t.Helper()
	ctx := context.Background()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := kv.StoreMessage{Store: kvStore}
	return ref.NewKVRefManager(batch.NopExecutor(), storeMessage, addressProvider), storeMessage
}

func testRefManagerWithAddressProvider(t testing.TB, addressProvider ident.AddressProvider) []DBType {
	dbRefManager, _ := testRefManagerWithDBAndAddressProvider(t, addressProvider)
	kvRefManager, _ := testRefManagerWithKVAndAddressProvider(t, addressProvider)

	tests := []DBType{
		{
			name:       "DB ref manager test with address provider",
			refManager: dbRefManager,
		},
		{
			name:       "KV ref manager test with address provider",
			refManager: kvRefManager,
		},
	}
	return tests
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !testing.Verbose() {
		// keep the log level calm
		logging.SetLevel("panic")
	}

	// postgres container
	var err error
	pool, err = dockertest.NewPool("")
	if err != nil {
		logging.Default().Fatalf("Could not connect to Docker: %s", err)
	}
	var closer func()
	databaseURI, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}
