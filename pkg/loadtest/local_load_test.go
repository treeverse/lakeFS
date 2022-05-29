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
	"github.com/spf13/viper"
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
	"github.com/treeverse/lakefs/pkg/email"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/version"
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

func (m *nullCollector) Close() {}

type getService func(t *testing.T, ctx context.Context, source actions.Source, writer actions.OutputWriter, stats stats.Collector, runHooks bool) actions.Service

func GetDBService(t *testing.T, ctx context.Context, source actions.Source, writer actions.OutputWriter, stats stats.Collector, runHooks bool) actions.Service {
	t.Helper()
	conn, _ := testutil.GetDB(t, databaseURI)
	return actions.NewDBService(ctx, conn, source, writer, stats, runHooks)
}

func GetKVService(t *testing.T, ctx context.Context, source actions.Source, writer actions.OutputWriter, stats stats.Collector, runHooks bool) actions.Service {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	return actions.NewKVService(ctx, kv.StoreMessage{Store: kvStore}, source, writer, stats, runHooks)
}

func TestLocalLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping loadtest tests in short mode")
	}

	// Only once
	ctx := context.Background()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)
	conf, err := config.NewConfig()
	testutil.MustDo(t, "config", err)
	conn, _ := testutil.GetDB(t, databaseURI)
	authService := auth.NewDBAuthService(conn, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{}, logging.Default().WithField("service", "auth"))
	superuser := &authmodel.SuperuserConfiguration{
		User: authmodel.User{
			CreatedAt: time.Now(),
			Username:  "admin",
		},
	}
	credentials, err := auth.SetupAdminUser(ctx, authService, superuser)
	testutil.Must(t, err)

	tests := []struct {
		name           string
		actionsService getService
	}{
		{
			name:           "DB service test",
			actionsService: GetDBService,
		},
		{
			name:           "KV service test",
			actionsService: GetKVService,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockstoreType, _ := os.LookupEnv(testutil.EnvKeyUseBlockAdapter)
			if blockstoreType == "" {
				blockstoreType = "mem"
			}

			blockAdapter := testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, blockstoreType)
			c, err := catalog.New(ctx, catalog.Config{
				Config: conf,
				DB:     conn,
			})
			testutil.MustDo(t, "build catalog", err)

			source := catalog.NewActionsSource(c)
			outputWriter := catalog.NewActionsOutputWriter(c.BlockAdapter)

			// wire actions
			actionsService := tt.actionsService(t, ctx, source, outputWriter, &nullCollector{}, true)
			c.SetHooksHandler(actionsService)

			authenticator := auth.NewBuiltinAuthenticator(authService)
			meta := auth.NewDBMetadataManager("dev", conf.GetFixedInstallationID(), conn)
			migrator := db.NewDatabaseMigrator(dbparams.Database{ConnectionString: databaseURI})
			t.Cleanup(func() {
				_ = c.Close()
			})
			auditChecker := version.NewDefaultAuditChecker(conf.GetSecurityAuditCheckURL())
			emailParams, _ := conf.GetEmailParams()
			emailer, err := email.NewEmailer(emailParams)
			testutil.Must(t, err)
			handler := api.Serve(
				conf,
				c,
				authenticator,
				authenticator,
				authService,
				blockAdapter,
				meta,
				migrator,
				&nullCollector{},
				nil,
				actionsService,
				auditChecker,
				logging.Default(),
				emailer,
				nil,
				nil,
			)

			ts := httptest.NewServer(handler)
			defer ts.Close()

			testConfig := Config{
				FreqPerSecond:    6,
				Duration:         10 * time.Second,
				MaxWorkers:       math.MaxInt64,
				KeepRepo:         false,
				Credentials:      *credentials,
				ServerAddress:    ts.URL,
				StorageNamespace: "mem://local/test/",
			}
			loader := NewLoader(testConfig)
			err = loader.Run()
			if err != nil {
				t.Fatalf("Got error on test: %s", err)
			}
		})
	}
}
