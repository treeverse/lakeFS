package api_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/treeverse/lakefs/logging"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/setup"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	authmodel "github.com/treeverse/lakefs/auth/model"
	authparams "github.com/treeverse/lakefs/auth/params"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/mem"
	"github.com/treeverse/lakefs/catalog"
	catalogfactory "github.com/treeverse/lakefs/catalog/factory"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/db"
	dbparams "github.com/treeverse/lakefs/db/params"
	"github.com/treeverse/lakefs/stats"
	"github.com/treeverse/lakefs/testutil"
)

const ServerTimeout = 30 * time.Second

type dependencies struct {
	blocks      block.Adapter
	cataloger   catalog.Cataloger
	authService *auth.DBAuthService
}

type nullCollector struct{}

func (m *nullCollector) CollectMetadata(_ *stats.Metadata) {}

func (m *nullCollector) CollectEvent(_, _ string) {}

func (m *nullCollector) SetInstallationID(_ string) {}

func createDefaultAdminUser(t *testing.T, clt *client.Lakefs) *authmodel.Credential {
	t.Helper()
	res, err := clt.Setup.SetupLakeFS(
		setup.NewSetupLakeFSParamsWithTimeout(10 * time.Second).
			WithUser(&models.Setup{
				Username: swag.String("admin"),
			}))
	testutil.Must(t, err)
	return &authmodel.Credential{
		IssuedDate:      time.Unix(res.Payload.CreationDate, 0),
		AccessKeyID:     res.Payload.AccessKeyID,
		AccessSecretKey: res.Payload.AccessSecretKey,
	}
}

func setupHandler(t testing.TB, blockstoreType string, opts ...testutil.GetDBOption) (http.Handler, *dependencies) {
	t.Helper()
	conn, handlerDatabaseURI := testutil.GetDB(t, databaseURI, opts...)
	if blockstoreType == "" {
		blockstoreType = os.Getenv(testutil.EnvKeyUseBlockAdapter)
	}
	if blockstoreType == "" {
		blockstoreType = mem.BlockstoreType
	}
	blockAdapter := testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, blockstoreType)
	cfg := config.NewConfig()
	cfg.Override(func(configurator config.Configurator) {
		configurator.SetDefault(config.BlockstoreTypeKey, mem.BlockstoreType)
	})
	cataloger, err := catalogfactory.BuildCataloger(conn, cfg)
	testutil.MustDo(t, "build cataloger", err)

	authService := auth.NewDBAuthService(conn, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{
		Enabled: false,
	})
	meta := auth.NewDBMetadataManager("dev", conn)
	migrator := db.NewDatabaseMigrator(dbparams.Database{ConnectionString: handlerDatabaseURI})

	t.Cleanup(func() {
		_ = cataloger.Close()
	})

	handler := api.Serve(api.Dependencies{
		Cataloger:       cataloger,
		Auth:            authService,
		BlockAdapter:    blockAdapter,
		Parade:          nil,
		MetadataManager: meta,
		Migrator:        migrator,
		Collector:       &nullCollector{},
		Logger:          logging.Default(),
	})

	return handler, &dependencies{
		blocks:      blockAdapter,
		authService: authService,
		cataloger:   cataloger,
	}
}

func setupClientFromHandler(t testing.TB, handler http.Handler) *client.Lakefs {
	t.Helper()
	server := httptest.NewServer(http.TimeoutHandler(handler, ServerTimeout, `{"error": "timeout"}`))
	t.Cleanup(server.Close)

	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal("parse httptest.Server url:", err)
	}
	clt := client.NewHTTPClientWithConfig(
		nil,
		client.DefaultTransportConfig().WithHost(u.Host).WithSchemes([]string{u.Scheme}),
	)
	return clt
}

func setupClient(t testing.TB, blockstoreType string, opts ...testutil.GetDBOption) (*client.Lakefs, *dependencies) {
	t.Helper()
	handler, deps := setupHandler(t, blockstoreType, opts...)
	clt := setupClientFromHandler(t, handler)
	return clt, deps
}
