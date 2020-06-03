package benchmark

import (
	"context"
	"errors"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/testutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"
)

const listenAddress = "localhost:8981"

var (
	pool        *dockertest.Pool
	databaseUri string
)

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	databaseUri, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

type mockCollector struct{}

func (m *mockCollector) Collect(_, _ string) {}

func TestLocalBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping benchmark tests in short mode")
	}
	mdb, mdbURI := testutil.GetDB(t, databaseUri, config.SchemaMetadata)
	blockAdapter := testutil.GetBlockAdapter(t, &block.NoOpTranslator{})

	meta := index.NewDBIndex(mdb)

	adb, adbURI := testutil.GetDB(t, databaseUri, config.SchemaAuth)
	authService := auth.NewDBAuthService(adb, crypt.NewSecretStore([]byte("some secret")))
	migrator := db.NewDatabaseMigrator().
		AddDB(config.SchemaMetadata, mdbURI).
		AddDB(config.SchemaAuth, adbURI)
	server := api.NewServer(
		meta,
		blockAdapter,
		authService,
		&mockCollector{},
		migrator,
	)

	go func() {
		err := server.Listen(listenAddress)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("error from lakeFS server: %v", err)
		}
	}()
	credentials := testutil.CreateDefaultAdminUser(authService, t)

	testConfig := Config{
		FreqPerSecond: 6,
		Duration:      10 * time.Second,
		KeepRepo:      false,
		Credentials:   *credentials,
		ServerAddress: "http://" + listenAddress,
	}
	time.Sleep(1 * time.Second)
	benchmark := NewBenchmark(testConfig)
	err := benchmark.Run()
	if err != nil {
		t.Fatalf("Got error on test: %v", err)
	}
	err = server.Shutdown(context.Background())
	if err != nil {
		t.Logf("Error when trying to shutdown lakeFS server: %v", err)
	}
}
