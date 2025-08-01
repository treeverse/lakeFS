package loadtest

import (
	"context"
	"math"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"
	configfactory "github.com/treeverse/lakefs/modules/config/factory"
	licensefactory "github.com/treeverse/lakefs/modules/license/factory"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authmodel "github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/auth/setup"
	"github.com/treeverse/lakefs/pkg/authentication"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
	"github.com/treeverse/lakefs/pkg/version"
)

func TestLocalLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping loadtest tests in short mode")
	}

	// Only once
	ctx := context.Background()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)

	cfg := &configfactory.ConfigImpl{}
	baseCfg, err := config.NewConfig("", cfg)
	testutil.MustDo(t, "config", err)

	superuser := &authmodel.SuperuserConfiguration{
		User: authmodel.User{
			CreatedAt: time.Now(),
			Username:  "admin",
		},
	}

	kvStore := kvtest.GetStore(ctx, t)
	authService := auth.NewBasicAuthService(kvStore, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{}, logging.FromContext(ctx))
	meta := auth.NewKVMetadataManager("local_load_test", baseCfg.Installation.FixedID, baseCfg.Database.Type, kvStore)

	blockstoreType := os.Getenv(testutil.EnvKeyUseBlockAdapter)
	if blockstoreType == "" {
		blockstoreType = "mem"
	}

	blockAdapter := testutil.NewBlockAdapterByType(t, blockstoreType)
	c, err := catalog.New(ctx, catalog.Config{
		Config:       cfg,
		KVStore:      kvStore,
		PathProvider: upload.DefaultPathProvider,
	})
	testutil.MustDo(t, "build catalog", err)

	source := catalog.NewActionsSource(c)
	outputWriter := catalog.NewActionsOutputWriter(c.BlockAdapter)

	// wire actions
	actionsService := actions.NewService(ctx, actions.NewActionsKVStore(kvStore), source, outputWriter, &actions.DecreasingIDGenerator{}, &stats.NullCollector{}, actions.Config{Enabled: true}, "")
	c.SetHooksHandler(actionsService)

	credentials, err := setup.AddAdminUser(ctx, authService, superuser, false)
	testutil.Must(t, err)

	authenticator := auth.NewBuiltinAuthenticator(authService)
	kvParams, err := kvparams.NewConfig(&baseCfg.Database)
	testutil.Must(t, err)
	migrator := kv.NewDatabaseMigrator(kvParams)
	t.Cleanup(func() {
		_ = c.Close()
	})
	auditChecker := version.NewDefaultAuditChecker(baseCfg.Security.AuditCheckURL, "", nil)
	authenticationService := authentication.NewDummyService()
	licenseManager, _ := licensefactory.NewLicenseManager(ctx, cfg)
	handler := api.Serve(cfg, c, authenticator, authService, authenticationService, blockAdapter, meta, migrator, &stats.NullCollector{}, actionsService, auditChecker, logging.ContextUnavailable(), nil, nil, upload.DefaultPathProvider, stats.DefaultUsageReporter, licenseManager)

	ts := httptest.NewServer(handler)
	defer ts.Close()

	testConfig := Config{
		FreqPerSecond:    6,
		Duration:         5 * time.Second,
		MaxWorkers:       math.MaxInt64,
		KeepRepo:         false,
		Credentials:      *credentials,
		ServerAddress:    ts.URL,
		StorageNamespace: "mem://local/test/",
		ShowProgress:     testing.Verbose(),
	}
	loader := NewLoader(testConfig)
	err = loader.Run()
	if err != nil {
		t.Fatalf("Got error on test: %s", err)
	}
}
