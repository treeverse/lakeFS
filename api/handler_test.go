package api_test

import (
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/treeverse/lakefs/config"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/swag"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/api/gen/client/setup"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	authmodel "github.com/treeverse/lakefs/auth/model"
	authparams "github.com/treeverse/lakefs/auth/params"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	catalogfactory "github.com/treeverse/lakefs/catalog/factory"
	"github.com/treeverse/lakefs/db"
	dbparams "github.com/treeverse/lakefs/db/params"
	"github.com/treeverse/lakefs/dedup"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/stats"
	"github.com/treeverse/lakefs/testutil"
)

const (
	DefaultUserID = "example_user"
	ServerTimeout = 30 * time.Second
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

type dependencies struct {
	blocks      block.Adapter
	cataloger   catalog.Cataloger
	authService auth.Service
}

func createDefaultAdminUser(clt *client.Lakefs, t *testing.T) *authmodel.Credential {
	params := setup.NewSetupLakeFSParamsWithTimeout(10 * time.Second).
		WithUser(&models.Setup{Username: swag.String("admin")})
	res, err := clt.Setup.SetupLakeFS(params)
	testutil.Must(t, err)
	return &authmodel.Credential{
		IssuedDate:      time.Unix(res.Payload.CreationDate, 0),
		AccessKeyID:     res.Payload.AccessKeyID,
		AccessSecretKey: res.Payload.AccessSecretKey,
	}
}

type mockCollector struct{}

func (m *mockCollector) CollectMetadata(_ *stats.Metadata) {}

func (m *mockCollector) CollectEvent(_, _ string) {}

func (m *mockCollector) SetInstallationID(_ string) {}

func getHandler(t *testing.T, blockstoreType string, opts ...testutil.GetDBOption) (http.Handler, *dependencies) {
	conn, handlerDatabaseURI := testutil.GetDB(t, databaseURI, opts...)
	var blockAdapter block.Adapter
	if blockstoreType == "" {
		blockstoreType, _ = os.LookupEnv(testutil.EnvKeyUseBlockAdapter)
	}
	if blockstoreType == "" {
		blockstoreType = "mem"
	}
	blockAdapter = testutil.NewBlockAdapterByType(t, &block.NoOpTranslator{}, blockstoreType)
	cfg := config.NewConfig()
	cfg.Override(func(configurator config.Configurator) {
		configurator.SetDefault(config.BlockstoreTypeKey, "mem")
	})
	cataloger, err := catalogfactory.BuildCataloger(conn, cfg)
	testutil.MustDo(t, "build cataloger", err)

	authService := auth.NewDBAuthService(conn, crypt.NewSecretStore([]byte("some secret")), authparams.ServiceCache{
		Enabled: false,
	})
	meta := auth.NewDBMetadataManager("dev", conn)
	migrator := db.NewDatabaseMigrator(dbparams.Database{ConnectionString: handlerDatabaseURI})

	dedupCleaner := dedup.NewCleaner(blockAdapter, cataloger.DedupReportChannel())
	t.Cleanup(func() {
		// order is important - close cataloger channel before dedup
		_ = cataloger.Close()
		_ = dedupCleaner.Close()
	})

	handler := api.NewHandler(
		cataloger,
		blockAdapter,
		authService,
		meta,
		&mockCollector{},
		migrator,
		nil,
		dedupCleaner,
		logging.Default(),
	)

	return handler, &dependencies{
		blocks:      blockAdapter,
		cataloger:   cataloger,
		authService: authService,
	}
}

func getClient(t *testing.T, s *httptest.Server) *client.Lakefs {
	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatal("parse httptest.Server url:", err)
	}
	return client.NewHTTPClientWithConfig(
		nil,
		client.DefaultTransportConfig().WithHost(u.Host).WithSchemes([]string{u.Scheme}),
	)
}

func NewClient(t *testing.T, blockstoreType string, opts ...testutil.GetDBOption) (*client.Lakefs, *dependencies) {
	handler, deps := getHandler(t, blockstoreType, opts...)
	server := httptest.NewServer(http.TimeoutHandler(handler, ServerTimeout, `{"error": "timeout"}`))
	t.Cleanup(server.Close)
	return getClient(t, server), deps
}

func TestServer_BasicAuth(t *testing.T) {
	clt, _ := NewClient(t, "")

	// create user
	creds := createDefaultAdminUser(clt, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyID, creds.AccessSecretKey)

	t.Run("valid Auth", func(t *testing.T) {
		_, err := clt.Repositories.ListRepositories(repositories.NewListRepositoriesParamsWithTimeout(timeout), bauth)
		if err != nil {
			t.Fatalf("unexpected error \"%s\" when passing valid credentials", err)
		}
	})

	t.Run("invalid Auth secret", func(t *testing.T) {
		_, err := clt.Repositories.ListRepositories(repositories.NewListRepositoriesParams().WithTimeout(timeout), httptransport.BasicAuth(creds.AccessKeyID, "foobarbaz"))
		var unauthErr *repositories.ListRepositoriesUnauthorized
		if !errors.As(err, &unauthErr) {
			t.Fatalf("got %s not unauthorized error", err)
		}
	})
}

func TestServer_JwtTokenAuth(t *testing.T) {
	clt, deps := NewClient(t, "")
	creds := createDefaultAdminUser(clt, t)

	t.Run("valid token", func(t *testing.T) {
		now := time.Now()
		exp := now.Add(10 * time.Minute)
		token, err := api.CreateAuthToken(deps.authService, creds.AccessKeyID, now, exp)
		testutil.MustDo(t, "create auth token", err)
		authInfo := httptransport.APIKeyAuth(api.JWTAuthorizationHeaderName, "header", token)
		_, err = clt.Repositories.ListRepositories(repositories.NewListRepositoriesParamsWithTimeout(timeout), authInfo)
		testutil.MustDo(t, "Request expected to work using a valid token", err)
	})

	t.Run("invalid token", func(t *testing.T) {
		now := time.Now()
		exp := now.Add(10 * time.Minute)
		token, err := api.CreateAuthToken(deps.authService, "admin", now, exp)
		testutil.MustDo(t, "create auth token", err)
		authInfo := httptransport.APIKeyAuth(api.JWTAuthorizationHeaderName, "header", token)
		_, err = clt.Repositories.ListRepositories(repositories.NewListRepositoriesParams().WithTimeout(timeout), authInfo)
		var unauthorized *repositories.ListRepositoriesUnauthorized
		if !errors.As(err, &unauthorized) {
			t.Fatalf("got %s not unauthorized error", err)
		}
	})
}
