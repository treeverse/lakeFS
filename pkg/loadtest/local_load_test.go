package loadtest

import (
	"context"
	"math"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/email"
	authmodel "github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/kv"
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
	viper.Set("blockstore.type", block.BlockstoreTypeLocal)

	conf, err := config.NewConfig()
	testutil.MustDo(t, "config", err)

	superuser := &authmodel.SuperuserConfiguration{
		User: authmodel.User{
			CreatedAt: time.Now(),
			Username:  "admin",
		},
	}

	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := &kv.StoreMessage{Store: kvStore}
	authService := auth.NewAuthService(storeMessage, crypt.NewSecretStore([]byte("some secret")), nil, authparams.ServiceCache{}, logging.Default().WithField("service", "auth"))
	meta := auth.NewKVMetadataManager("local_load_test", conf.Installation.FixedID, conf.Database.Type, kvStore)

	blockstoreType := os.Getenv(testutil.EnvKeyUseBlockAdapter)
	if blockstoreType == "" {
		blockstoreType = "mem"
	}

	blockAdapter := testutil.NewBlockAdapterByType(t, blockstoreType)
	c, err := catalog.New(ctx, catalog.Config{
		Config:       conf,
		KVStore:      storeMessage,
		PathProvider: upload.DefaultPathProvider,
		Limiter:      conf.NewGravelerBackgroundLimiter(),
	})
	testutil.MustDo(t, "build catalog", err)

	source := catalog.NewActionsSource(c)
	outputWriter := catalog.NewActionsOutputWriter(c.BlockAdapter)

	// wire actions
	actionsService := actions.NewService(ctx, actions.NewActionsKVStore(*storeMessage), source, outputWriter, &actions.DecreasingIDGenerator{}, &stats.NullCollector{}, true)
	c.SetHooksHandler(actionsService)

	credentials, err := auth.SetupAdminUser(ctx, authService, superuser)
	testutil.Must(t, err)

	authenticator := auth.NewBuiltinAuthenticator(authService)
	kvParams, err := conf.DatabaseParams()
	testutil.Must(t, err)
	migrator := kv.NewDatabaseMigrator(kvParams)
	t.Cleanup(func() {
		_ = c.Close()
	})
	auditChecker := version.NewDefaultAuditChecker(conf.Security.AuditCheckURL, "")
	emailer, err := email.NewEmailer(email.Params(conf.Email))
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
		&stats.NullCollector{},
		nil,
		actionsService,
		auditChecker,
		logging.Default(),
		emailer,
		nil,
		nil,
		nil,
		nil,
		nil,
		upload.DefaultPathProvider,
	)

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
