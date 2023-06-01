package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/swag"
	tablediff "github.com/treeverse/lakefs/pkg/plugins/diff"

	"github.com/deepmap/oapi-codegen/pkg/securityprovider"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/actions"
	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/auth"
	"github.com/treeverse/lakefs/pkg/auth/crypt"
	"github.com/treeverse/lakefs/pkg/auth/email"
	authmodel "github.com/treeverse/lakefs/pkg/auth/model"
	authparams "github.com/treeverse/lakefs/pkg/auth/params"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/graveler/settings"
	"github.com/treeverse/lakefs/pkg/ingest/store"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvtest"
	"github.com/treeverse/lakefs/pkg/kv/mem"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/stats"
	"github.com/treeverse/lakefs/pkg/templater"
	"github.com/treeverse/lakefs/pkg/testutil"
	"github.com/treeverse/lakefs/pkg/upload"
	"github.com/treeverse/lakefs/pkg/version"
	"github.com/treeverse/lakefs/templates"
)

const (
	ServerTimeout = 30 * time.Second
)

type dependencies struct {
	blocks      block.Adapter
	catalog     catalog.Interface
	authService auth.Service
	collector   *memCollector
	server      *httptest.Server
}

// memCollector in-memory collector stores events and metadata sent
type memCollector struct {
	Metrics        []*stats.Metric
	Metadata       []*stats.Metadata
	InstallationID string
	mu             sync.Mutex
}

func (m *memCollector) CollectEvents(ev stats.Event, count uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Metrics = append(m.Metrics, &stats.Metric{Event: ev, Value: count})
}

func (m *memCollector) CollectEvent(ev stats.Event) {
	m.CollectEvents(ev, 1)
}

func (m *memCollector) CollectMetadata(accountMetadata *stats.Metadata) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Metadata = append(m.Metadata, accountMetadata)
}

func (m *memCollector) SetInstallationID(installationID string) {
	m.InstallationID = installationID
}

func (m *memCollector) CollectCommPrefs(email, installationID string, featureUpdates, securityUpdates bool) {
}

func (m *memCollector) Close() {}

func setupCommPrefs(t testing.TB, clt api.ClientWithResponsesInterface) *api.NextStep {
	t.Helper()
	mockEmail := "test@acme.co"
	res, err := clt.SetupCommPrefsWithResponse(context.Background(), api.SetupCommPrefsJSONRequestBody{
		Email:           &mockEmail,
		FeatureUpdates:  false,
		SecurityUpdates: false,
	})
	testutil.Must(t, err)
	if res.JSON200 == nil {
		t.Fatal("Failed to setup comm prefs", res.HTTPResponse.StatusCode, res.HTTPResponse.Status)
	}
	return &api.NextStep{
		NextStep: "comm_prefs_done",
	}
}

func createDefaultAdminUser(t testing.TB, clt api.ClientWithResponsesInterface) *authmodel.BaseCredential {
	t.Helper()
	res, err := clt.SetupWithResponse(context.Background(), api.SetupJSONRequestBody{
		Username: "admin",
	})
	testutil.Must(t, err)
	if res.JSON200 == nil {
		t.Fatal("Failed run setup env", res.HTTPResponse.StatusCode, res.HTTPResponse.Status)
	}
	return &authmodel.BaseCredential{
		IssuedDate:      time.Unix(res.JSON200.CreationDate, 0),
		AccessKeyID:     res.JSON200.AccessKeyId,
		SecretAccessKey: res.JSON200.SecretAccessKey,
	}
}

func createUserWithDefaultGroup(t testing.TB, clt api.ClientWithResponsesInterface) *authmodel.BaseCredential {
	t.Helper()
	// create the user
	createUsrRes, err := clt.CreateUserWithResponse(context.Background(), api.CreateUserJSONRequestBody{
		Id:         "test@example.com",
		InviteUser: swag.Bool(false),
	})
	testutil.Must(t, err)
	if createUsrRes.JSON201 == nil {
		t.Fatal("Failed to create user", createUsrRes.HTTPResponse.StatusCode, createUsrRes.HTTPResponse.Status)
	}

	// create credentials for the user
	createCredsRes, err := clt.CreateCredentialsWithResponse(context.Background(), createUsrRes.JSON201.Id)
	testutil.Must(t, err)
	if createCredsRes.JSON201 == nil {
		t.Fatal("Failed to create credentials", createCredsRes.HTTPResponse.StatusCode, createCredsRes.HTTPResponse.Status)
	}

	return &authmodel.BaseCredential{
		IssuedDate:      time.Unix(createCredsRes.JSON201.CreationDate, 0),
		AccessKeyID:     createCredsRes.JSON201.AccessKeyId,
		SecretAccessKey: createCredsRes.JSON201.SecretAccessKey,
	}
}

func setupHandlerWithWalkerFactory(t testing.TB, factory catalog.WalkerFactory) (http.Handler, *dependencies) {
	t.Helper()
	ctx := context.Background()

	if viper.Get(config.BlockstoreTypeKey) == nil {
		viper.Set(config.BlockstoreTypeKey, block.BlockstoreTypeMem)
	}
	viper.Set("database.type", mem.DriverName)
	// Use 'internal' mode in order to have access to policies
	viper.Set("auth.ui_config.rbac", "internal")

	collector := &memCollector{}

	cfg, err := config.NewConfig()
	testutil.MustDo(t, "config", err)
	kvStore := kvtest.GetStore(ctx, t)
	actionsStore := actions.NewActionsKVStore(kvStore)
	idGen := &actions.DecreasingIDGenerator{}
	authService := auth.NewAuthService(kvStore, crypt.NewSecretStore([]byte("some secret")), nil, authparams.ServiceCache{
		Enabled: false,
	}, logging.Default())
	meta := auth.NewKVMetadataManager("serve_test", cfg.Installation.FixedID, cfg.Database.Type, kvStore)

	// Do not validate invalid config (missing required fields).
	c, err := catalog.New(ctx, catalog.Config{
		Config:                cfg,
		KVStore:               kvStore,
		WalkerFactory:         factory,
		SettingsManagerOption: settings.WithCache(cache.NoCache),
		PathProvider:          upload.DefaultPathProvider,
		Limiter:               cfg.NewGravelerBackgroundLimiter(),
	})
	testutil.MustDo(t, "build catalog", err)

	// wire actions
	actionsConfig := actions.Config{Enabled: true}
	actionsConfig.Lua.NetHTTPEnabled = true
	actionsService := actions.NewService(
		ctx,
		actionsStore,
		catalog.NewActionsSource(c),
		catalog.NewActionsOutputWriter(c.BlockAdapter),
		idGen,
		collector,
		actionsConfig,
	)

	c.SetHooksHandler(actionsService)

	authenticator := auth.NewBuiltinAuthenticator(authService)
	kvParams, err := cfg.DatabaseParams()
	testutil.Must(t, err)
	migrator := kv.NewDatabaseMigrator(kvParams)

	t.Cleanup(func() {
		actionsService.Stop()
		_ = c.Close()
	})

	auditChecker := version.NewDefaultAuditChecker(cfg.Security.AuditCheckURL, "", nil)
	emailer, err := email.NewEmailer(email.Params(cfg.Email))
	tmpl := templater.NewService(templates.Content, cfg, authService)

	otfDiffService := tablediff.NewMockService()

	testutil.Must(t, err)
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
		emailer,
		tmpl,
		nil,
		nil,
		upload.DefaultPathProvider,
		otfDiffService,
	)

	return handler, &dependencies{
		blocks:      c.BlockAdapter,
		authService: authService,
		catalog:     c,
		collector:   collector,
	}
}

func setupHandler(t testing.TB) (http.Handler, *dependencies) {
	return setupHandlerWithWalkerFactory(t, store.NewFactory(nil))
}

func setupClientByEndpoint(t testing.TB, endpointURL string, accessKeyID, secretAccessKey string, opts ...api.ClientOption) api.ClientWithResponsesInterface {
	t.Helper()

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

func setupClientWithAdmin(t testing.TB) (api.ClientWithResponsesInterface, *dependencies) {
	t.Helper()
	return setupClientWithAdminAndWalkerFactory(t, store.NewFactory(nil))
}

func setupClientWithAdminAndWalkerFactory(t testing.TB, factory catalog.WalkerFactory) (api.ClientWithResponsesInterface, *dependencies) {
	t.Helper()
	handler, deps := setupHandlerWithWalkerFactory(t, factory)
	server := setupServer(t, handler)
	deps.server = server
	clt := setupClientByEndpoint(t, server.URL, "", "")
	_ = setupCommPrefs(t, clt)
	cred := createDefaultAdminUser(t, clt)
	clt = setupClientByEndpoint(t, server.URL, cred.AccessKeyID, cred.SecretAccessKey)
	return clt, deps
}

func TestInvalidRoute(t *testing.T) {
	handler, _ := setupHandler(t)
	server := setupServer(t, handler)
	clt := setupClientByEndpoint(t, server.URL, "", "")
	_ = setupCommPrefs(t, clt)
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
