package loadtest

import (
	"context"
	"log"
	"math"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authmodel "github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	dbparams "github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
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
	ctx := context.Background()
	conn, _ := testutil.GetDB(t, databaseURI)
	blockstoreType, _ := os.LookupEnv(testutil.EnvKeyUseBlockAdapter)
	if blockstoreType == "" {
		blockstoreType = "mem"
	}
	blockAdapter := testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, blockstoreType)
	c, err := catalog.New(ctx, catalog.Config{
		Config: config.NewConfig(),
		DB:     conn,
	})
	testutil.MustDo(t, "build catalog", err)

	// wire actions
	actionsService := actions.NewService(
		conn,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(c.BlockAdapter),
	)
	c.SetHooksHandler(actionsService)

	authService := auth.NewDBAuthService(conn, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{})
	meta := auth.NewDBMetadataManager("dev", conn)
	migrator := db.NewDatabaseMigrator(dbparams.Database{ConnectionString: databaseURI})
	t.Cleanup(func() {
		_ = c.Close()
	})

	handler := api.Serve(
		c,
		authService,
		blockAdapter,
		meta,
		migrator,
		&nullCollector{},
		nil,
		actionsService,
		logging.Default(),
		"",
	)

	ts := httptest.NewServer(handler)
	defer ts.Close()

	superuser := &authmodel.SuperuserConfiguration{
		User: authmodel.User{
			CreatedAt: time.Now(),
			Username:  "admin",
		},
	}
	credentials, err := auth.SetupAdminUser(ctx, authService, superuser)
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
