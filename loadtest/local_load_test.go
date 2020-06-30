package loadtest

import (
	"log"
	"math"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/testutil"
)

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

func TestLocalLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping loadtest tests in short mode")
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
	handler, err := server.Handler()
	if err != nil {
		t.Fatalf("failed to get server handler: %s", err)
	}
	ts := httptest.NewServer(handler)
	defer ts.Close()

	user := &authmodel.User{
		CreatedAt:   time.Now(),
		DisplayName: "admin",
	}
	credentials, err := auth.SetupAdminUser(authService, user)
	testutil.Must(t, err)

	testConfig := Config{
		FreqPerSecond: 6,
		Duration:      10 * time.Second,
		MaxWorkers:    math.MaxInt64,
		KeepRepo:      false,
		Credentials:   *credentials,
		ServerAddress: ts.URL,
	}
	loader := NewLoader(testConfig)
	err = loader.Run()
	if err != nil {
		t.Fatalf("Got error on test: %s", err)
	}
}
