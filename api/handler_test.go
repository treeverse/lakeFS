package api_test

import (
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/retention"
	"github.com/treeverse/lakefs/testutil"
)

const (
	DefaultUserID = "example_user"
)

var (
	pool        *dockertest.Pool
	databaseUri string
)

func TestMain(m *testing.M) {
	var err error
	var closer func()
	pool, err = dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to Docker: %s", err)
	}
	databaseUri, closer = testutil.GetDBInstance(pool)
	code := m.Run()
	closer() // cleanup
	os.Exit(code)
}

type dependencies struct {
	blocks    block.Adapter
	auth      auth.Service
	cataloger catalog.Cataloger
}

func createDefaultAdminUser(authService auth.Service, t *testing.T) *authmodel.Credential {
	user := &authmodel.User{
		CreatedAt:   time.Now(),
		DisplayName: "admin",
	}

	creds, err := auth.SetupAdminUser(authService, user)
	testutil.Must(t, err)
	return creds
}

type mockCollector struct{}

func (m *mockCollector) SetInstallationID(installationID string) {}

func (m *mockCollector) CollectMetadata(accountMetadata map[string]string) {}

func (m *mockCollector) CollectEvent(_, _ string) {}

func getHandler(t *testing.T, opts ...testutil.GetDBOption) (http.Handler, *dependencies) {
	conn, handlerDatabaseURI := testutil.GetDB(t, databaseUri, opts...)
	blockAdapter := testutil.GetBlockAdapter(t, &block.NoOpTranslator{})

	cataloger := catalog.NewCataloger(conn)
	authService := auth.NewDBAuthService(conn, crypt.NewSecretStore([]byte("some secret")), auth.ServiceCacheConfig{
		Enabled: false,
	})
	meta := auth.NewDBMetadataManager("dev", conn)
	retention := retention.NewService(conn)
	migrator := db.NewDatabaseMigrator(handlerDatabaseURI)
	handler := api.NewHandler(
		cataloger,
		blockAdapter,
		authService,
		meta,
		&mockCollector{},
		retention,
		migrator,
		logging.Default(),
	)

	return handler, &dependencies{
		blocks:    blockAdapter,
		auth:      authService,
		cataloger: cataloger,
	}
}

type roundTripper struct {
	Handler http.Handler
}

func (r *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	r.Handler.ServeHTTP(recorder, req)
	response := recorder.Result()
	return response, nil
}

type handlerTransport struct {
	Handler http.Handler
}

func (r *handlerTransport) Submit(op *runtime.ClientOperation) (interface{}, error) {
	clt := httptransport.NewWithClient("", "/api/v1", []string{"http"}, &http.Client{
		Transport: &roundTripper{r.Handler},
	})
	return clt.Submit(op)
}

func TestServer_BasicAuth(t *testing.T) {
	handler, deps := getHandler(t)

	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("valid Auth", func(t *testing.T) {
		_, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))
		if err != nil {
			t.Fatalf("did not expect error when passing valid credentials")
		}
	})

	t.Run("invalid Auth secret", func(t *testing.T) {
		_, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{}, httptransport.BasicAuth(creds.AccessKeyId, "foobarbaz"))
		if err == nil {
			t.Fatalf("expect error when passing invalid credentials")
		}
		errMsg, ok := err.(*repositories.ListRepositoriesUnauthorized)
		if !ok {
			t.Fatal("expected default error answer")
		}

		if !strings.EqualFold(errMsg.GetPayload().Message, "error authenticating request") {
			t.Fatalf("expected authentication error, got error: %s", errMsg.GetPayload().Message)
		}
	})
}
