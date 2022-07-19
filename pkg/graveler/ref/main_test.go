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
	conn, _ := testutil.GetDB(t, databaseURI, testutil.WithGetDBApplyDDL(true))
	return ref.NewKVRefManager(batch.NopExecutor(), storeMessage, conn, ident.NewHexAddressProvider()), storeMessage
}

func testRefManager(t *testing.T) []DBType {
	dbRefManager, _ := testRefManagerWithDB(t)
	kvRefManager, _ := testRefManagerWithKV(t)

	tests := []DBType{
		{
			name:       "DB ref manager test",
			refManager: dbRefManager,
		},
		{
			name:       "KV ref manager test",
			refManager: kvRefManager,
		},
	}
	return tests
}

func testRefManagerWithAddressProvider(t testing.TB, addressProvider ident.AddressProvider) (*ref.DBManager, db.Database) {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI, testutil.WithGetDBApplyDDL(true))
	return ref.NewPGRefManager(batch.NopExecutor(), conn, addressProvider), conn
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
