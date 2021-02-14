package loadtest

import (
	"log"
	"math"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/treeverse/lakefs/actions"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/actions"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	authmodel "github.com/treeverse/lakefs/auth/model"
	authparams "github.com/treeverse/lakefs/auth/params"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	dbparams "github.com/treeverse/lakefs/db/params"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/stats"
	"github.com/treeverse/lakefs/testutil"
)

var (
	pool        *dockertest.Pool
	databaseURI string
)

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	databaseURI, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

type nullCollector struct{}

func (m *nullCollector) CollectMetadata(_ *stats.Metadata) {}

func (m *nullCollector) CollectEvent(_, _ string) {}

func (m *nullCollector) SetInstallationID(_ string) {}

func TestLocalLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping loadtest tests in short mode")
	}
	conn, _ := testutil.GetDB(t, databaseURI)
	blockstoreType, _ := os.LookupEnv(testutil.EnvKeyUseBlockAdapter)
	if blockstoreType == "" {
		blockstoreType = "mem"
	}
	blockAdapter := testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, blockstoreType)
	actionsClient := actions.New(conn)
	cataloger, err := catalog.NewCataloger(conn, actionsClient, config.NewConfig())
	testutil.MustDo(t, "build cataloger", err)
	authService := auth.NewDBAuthService(conn, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{})
	meta := auth.NewDBMetadataManager("dev", conn)
	migrator := db.NewDatabaseMigrator(dbparams.Database{ConnectionString: databaseURI})
	t.Cleanup(func() {
		_ = cataloger.Close()
	})

	handler := api.Serve(api.Dependencies{
		Cataloger:       cataloger,
		Auth:            authService,
		BlockAdapter:    blockAdapter,
		MetadataManager: meta,
		Migrator:        migrator,
		Collector:       &nullCollector{},
		Logger:          logging.Default(),
	})

	ts := httptest.NewServer(handler)
	defer ts.Close()

	superuser := &authmodel.SuperuserConfiguration{
		User: authmodel.User{
			CreatedAt: time.Now(),
			Username:  "admin",
		},
	}
	credentials, err := auth.SetupAdminUser(authService, superuser)
	testutil.Must(t, err)

	testConfig := Config{
		FreqPerSecond:    6,
		Duration:         10 * time.Second,
		MaxWorkers:       math.MaxInt64,
		KeepRepo:         false,
		Credentials:      *credentials,
		ServerAddress:    ts.URL,
		StorageNamespace: "s3://local/test/",
	}
	loader := NewLoader(testConfig)
	err = loader.Run()
	if err != nil {
		t.Fatalf("Got error on test: %s", err)
	}
}
