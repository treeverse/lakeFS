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

type getActionsService func(t *testing.T, ctx context.Context, source actions.Source, writer actions.OutputWriter, stats stats.Collector, runHooks bool) actions.Service

func GetKVActionsService(t *testing.T, ctx context.Context, source actions.Source, writer actions.OutputWriter, stats stats.Collector, runHooks bool) actions.Service {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	return actions.NewService(ctx, actions.NewActionsKVStore(kv.StoreMessage{Store: kvStore}), source, writer, &actions.DecreasingIDGenerator{}, stats, runHooks)
}

func GetKVAuthService(t *testing.T, ctx context.Context) auth.Service {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	storeMessage := &kv.StoreMessage{Store: kvStore}
	return auth.NewKVAuthService(storeMessage, crypt.NewSecretStore([]byte("some secret")), nil, authparams.ServiceCache{}, logging.Default().WithField("service", "auth"))
}

func GetKVMetadataManager(t *testing.T, ctx context.Context, installationID, kvType string) auth.MetadataManager {
	t.Helper()
	kvStore := kvtest.GetStore(ctx, t)
	return auth.NewKVMetadataManager("local_load_test", installationID, kvType, kvStore)
}

func TestLocalLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping loadtest tests in short mode")
	}

	var storeMessage *kv.StoreMessage

	// Only once
	ctx := context.Background()
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeLocal)

	conf, err := config.NewConfig()
	testutil.MustDo(t, "config", err)

	tests := []struct {
		name           string
		actionsService getActionsService
		authService    auth.Service
		meta           auth.MetadataManager
		kvEnabled      bool
	}{
		{
			name:           "KV service test",
			actionsService: GetKVActionsService,
			authService:    GetKVAuthService(t, ctx),
			meta:           GetKVMetadataManager(t, ctx, conf.GetFixedInstallationID(), conf.GetDatabaseParams().Type),
			kvEnabled:      true,
		},
	}

	superuser := &authmodel.SuperuserConfiguration{
		User: authmodel.User{
			CreatedAt: time.Now(),
			Username:  "admin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockstoreType, _ := os.LookupEnv(testutil.EnvKeyUseBlockAdapter)
			if blockstoreType == "" {
				blockstoreType = "mem"
			}

			if tt.kvEnabled {
				kvStore := kvtest.GetStore(ctx, t)
				storeMessage = &kv.StoreMessage{Store: kvStore}
				viper.Set("database.kv_enabled", true)
			}

			blockAdapter := testutil.NewBlockAdapterByType(t, blockstoreType)
			c, err := catalog.New(ctx, catalog.Config{
				Config:  conf,
				KVStore: storeMessage,
			})
			testutil.MustDo(t, "build catalog", err)

			source := catalog.NewActionsSource(c)
			outputWriter := catalog.NewActionsOutputWriter(c.BlockAdapter)

			// wire actions
			actionsService := tt.actionsService(t, ctx, source, outputWriter, &stats.NullCollector{}, true)
			c.SetHooksHandler(actionsService)

			credentials, err := auth.SetupAdminUser(ctx, tt.authService, superuser)
			testutil.Must(t, err)

			authenticator := auth.NewBuiltinAuthenticator(tt.authService)
			kvParams, err := conf.GetKVParams()
			testutil.Must(t, err)
			migrator := kv.NewDatabaseMigrator(kvParams)
			t.Cleanup(func() {
				_ = c.Close()
			})
			auditChecker := version.NewDefaultAuditChecker(conf.GetSecurityAuditCheckURL(), "")
			emailParams, _ := conf.GetEmailParams()
			emailer, err := email.NewEmailer(emailParams)
			testutil.Must(t, err)
			handler := api.Serve(
				conf,
				c,
				authenticator,
				authenticator,
				tt.authService,
				blockAdapter,
				tt.meta,
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
