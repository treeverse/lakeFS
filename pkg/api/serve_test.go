package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	authmodel "github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	dbparams "github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
)

const (
	ServerTimeout = 30 * time.Second
)

type dependencies struct {
	blocks      block.Adapter
	catalog     catalog.Interface
	authService *auth.DBAuthService
	collector   *nullCollector
}

type nullCollector struct {
	metadata []*stats.Metadata
}

func (m *nullCollector) CollectMetadata(metadata *stats.Metadata) {
	m.metadata = append(m.metadata, metadata)
}

func (m *nullCollector) CollectEvent(_, _ string) {}

func (m *nullCollector) SetInstallationID(_ string) {}

func createDefaultAdminUser(t testing.TB, clt api.ClientWithResponsesInterface) *authmodel.Credential {
	t.Helper()
	res, err := clt.SetupWithResponse(context.Background(), api.SetupJSONRequestBody{
		Username: "admin",
	})
	testutil.Must(t, err)
	if res.JSON200 == nil {
		t.Fatal("Failed run setup env", res.HTTPResponse.StatusCode, res.HTTPResponse.Status)
	}
	return &authmodel.Credential{
		IssuedDate:      time.Unix(res.JSON200.CreationDate, 0),
		AccessKeyID:     res.JSON200.AccessKeyId,
		SecretAccessKey: res.JSON200.SecretAccessKey,
	}
}

func setupHandler(t testing.TB, blockstoreType string, opts ...testutil.GetDBOption) (http.Handler, *dependencies) {
	t.Helper()
	ctx := context.Background()
	conn, handlerDatabaseURI := testutil.GetDB(t, databaseURI, opts...)
	if blockstoreType == "" {
		blockstoreType = os.Getenv(testutil.EnvKeyUseBlockAdapter)
	}
	if blockstoreType == "" {
		blockstoreType = mem.BlockstoreType
	}
	blockAdapter := testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, blockstoreType)
	viper.Set(config.BlockstoreTypeKey, mem.BlockstoreType)
	cfg, err := config.NewConfig()
	testutil.MustDo(t, "config", err)
	c, err := catalog.New(ctx, catalog.Config{
		Config: cfg,
		DB:     conn,
	})
	testutil.MustDo(t, "build catalog", err)

	// wire actions
	actionsService := actions.NewService(
		conn,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(blockAdapter),
	)
	c.SetHooksHandler(actionsService)

	authService := auth.NewDBAuthService(conn, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{
		Enabled: false,
	})
	meta := auth.NewDBMetadataManager("dev", cfg.GetFixedInstallationID(), conn)
	migrator := db.NewDatabaseMigrator(dbparams.Database{ConnectionString: handlerDatabaseURI})

	t.Cleanup(func() {
		_ = c.Close()
	})

	collector := &nullCollector{}

	handler := api.Serve(
		c,
		authService,
		blockAdapter,
		meta,
		migrator,
		collector,
		nil,
		actionsService,
		logging.Default(),
		"",
	)

	return handler, &dependencies{
		blocks:      blockAdapter,
		authService: authService,
		catalog:     c,
		collector:   collector,
	}
}

func setupClientByEndpoint(t testing.TB, endpointURL string, accessKeyID, secretAccessKey string) api.ClientWithResponsesInterface {
	t.Helper()

	var opts []api.ClientOption
	if accessKeyID != "" {
		basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(accessKeyID, secretAccessKey)
		if err != nil {
			t.Fatal("basic auth security provider", err)
		}
		opts = append(opts, api.WithRequestEditorFn(basicAuthProvider.Intercept))
	}
	clt, err := api.NewClientWithResponses(endpointURL+api.BaseURL, opts...)
	if err != nil {
		t.Fatal("failed to create lakefs api client:", err)
	}
	return clt
}

func setupServer(t testing.TB, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.TimeoutHandler(handler, ServerTimeout, `{"error": "timeout"}`))
	t.Cleanup(server.Close)
	return server
}

func setupClientWithAdmin(t testing.TB, blockstoreType string, opts ...testutil.GetDBOption) (api.ClientWithResponsesInterface, *dependencies) {
	t.Helper()
	handler, deps := setupHandler(t, blockstoreType, opts...)
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)
	clt = setupClientByEndpoint(t, server.URL, cred.AccessKeyID, cred.SecretAccessKey)
	return clt, deps
}
