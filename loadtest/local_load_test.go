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
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/retention"
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

func (m *mockCollector) SetInstallationID(installationID string) {

}

func (m *mockCollector) CollectMetadata(accountMetadata map[string]string) {

}

func (m *mockCollector) CollectEvent(_, _ string) {}

func TestLocalLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping loadtest tests in short mode")
	}
	conn, _ := testutil.GetDB(t, databaseUri)
	blockAdapter := testutil.GetBlockAdapter(t, &block.NoOpTranslator{})
	cataloger := catalog.NewCataloger(conn)
	authService := auth.NewDBAuthService(conn, crypt.NewSecretStore([]byte("some secret")), auth.ServiceCacheConfig{})
	retention := retention.NewService(conn)
	meta := auth.NewDBMetadataManager("dev", conn)
	migrator := db.NewDatabaseMigrator(databaseUri)

	handler := api.NewHandler(
		cataloger,
		blockAdapter,
		authService,
		meta,
		&mockCollector{},
		retention,
		migrator,
		logging.Default(),
	)

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
