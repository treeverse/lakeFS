package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
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
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/db"
	dbparams "github.com/treeverse/lakefs/pkg/db/params"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/version"
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

func (m *nullCollector) Close() {}

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

func setupHandler(t testing.TB, opts ...testutil.GetDBOption) (http.Handler, *dependencies) {
	t.Helper()
	ctx := context.Background()
	conn, handlerDatabaseURI := testutil.GetDB(t, databaseURI, opts...)
	viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	cfg, err := config.NewConfig()
	testutil.MustDo(t, "config", err)
	// Do not validate invalid config (missing required fields).
	c, err := catalog.New(ctx, catalog.Config{
		Config: cfg,
		DB:     conn,
	})
	testutil.MustDo(t, "build catalog", err)

	collector := &nullCollector{}

	// wire actions
	actionsService := actions.NewService(
		ctx,
		conn,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(c.BlockAdapter),
		collector,
		true,
	)
	c.SetHooksHandler(actionsService)

	authService := auth.NewDBAuthService(conn, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{
		Enabled: false,
	})
	authenticator := auth.NewBuiltinAuthenticator(authService)
	meta := auth.NewDBMetadataManager("dev", cfg.GetFixedInstallationID(), conn)
	migrator := db.NewDatabaseMigrator(dbparams.Database{ConnectionString: handlerDatabaseURI})

	t.Cleanup(func() {
		_ = c.Close()
	})

	auditChecker := version.NewDefaultAuditChecker(cfg.GetSecurityAuditCheckURL())

	handler := api.Serve(
		cfg,
		c,
		authenticator,
		authService,
		c.BlockAdapter,
		meta,
		migrator,
		collector,
		nil,
		actionsService,
		auditChecker,
		logging.Default(),
		nil,
	)

	return handler, &dependencies{
		blocks:      c.BlockAdapter,
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
	if shouldUseServerTimeout() {
		handler = http.TimeoutHandler(handler, ServerTimeout, `{"error": "timeout"}`)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func shouldUseServerTimeout() bool {
	withServerTimeoutVal := os.Getenv("TEST_WITH_SERVER_TIMEOUT")
	if withServerTimeoutVal == "" {
		return true // default
	}
	withServerTimeout, err := strconv.ParseBool(withServerTimeoutVal)
	if err != nil {
		panic(fmt.Errorf("invalid TEST_WITH_SERVER_TIMEOUT value: %w", err))
	}
	return withServerTimeout
}

func setupClientWithAdmin(t testing.TB, opts ...testutil.GetDBOption) (api.ClientWithResponsesInterface, *dependencies) {
	t.Helper()
	handler, deps := setupHandler(t, opts...)
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)
	clt = setupClientByEndpoint(t, server.URL, cred.AccessKeyID, cred.SecretAccessKey)
	return clt, deps
}

func TestInvalidRoute(t *testing.T) {
	handler, _ := setupHandler(t)
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	cred := createDefaultAdminUser(t, clt)

	// setup client with invalid endpoint base url
	basicAuthProvider, err := securityprovider.NewSecurityProviderBasicAuth(cred.AccessKeyID, cred.SecretAccessKey)
	if err != nil {
		t.Fatal("basic auth security provider", err)
	}
	clt, err = api.NewClientWithResponses(server.URL+api.BaseURL+"//", api.WithRequestEditorFn(basicAuthProvider.Intercept))
	if err != nil {
		t.Fatal("failed to create api client:", err)
	}

	ctx := context.Background()
	resp, err := clt.ListRepositoriesWithResponse(ctx, &api.ListRepositoriesParams{})
	if err != nil {
		t.Fatalf("failed to get lakefs server version")
	}
	if resp.JSONDefault == nil {
		t.Fatalf("client api call expected default error, got nil")
	}
	expectedErrMsg := api.ErrInvalidAPIEndpoint.Error()
	errMsg := resp.JSONDefault.Message
	if errMsg != expectedErrMsg {
		t.Fatalf("client response error message: %s, expected: %s", errMsg, expectedErrMsg)
	}
}
