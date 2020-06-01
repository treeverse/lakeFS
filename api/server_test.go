package api_test

import (
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/treeverse/lakefs/db"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/ory/dockertest/v3"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/repositories"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/auth/crypt"
	"github.com/treeverse/lakefs/auth/model"
	authmodel "github.com/treeverse/lakefs/auth/model"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/config"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/testutil"
)

const (
	DefaultUserId = "example_user"
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
	blocks block.Adapter
	auth   auth.Service
	meta   index.Index
}

func createDefaultAdminUser(authService auth.Service, t *testing.T) *authmodel.Credential {
	// create user
	user := &authmodel.User{
		CreatedAt:   time.Now(),
		DisplayName: "admin",
	}
	testutil.Must(t, authService.CreateUser(user))

	// create role
	role := &authmodel.Role{
		DisplayName: "Admins",
	}
	testutil.Must(t, authService.CreateRole(role))

	// attach policies
	policy := &model.Policy{
		CreatedAt:   time.Now(),
		DisplayName: "AdminFullAccess",
		Action: []string{
			string(permissions.ManageRepos),
			string(permissions.ReadRepo),
			string(permissions.WriteRepo),
		},
		Resource: "arn:lakefs:repos:::*",
		Effect:   true,
	}

	testutil.Must(t, authService.CreatePolicy(policy))
	testutil.Must(t, authService.AttachPolicyToRole(role.DisplayName, policy.DisplayName))

	// assign user to role
	testutil.Must(t, authService.AttachRoleToUser(role.DisplayName, user.DisplayName))

	creds, err := authService.CreateCredentials(user.DisplayName)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

type mockCollector struct{}

func (m *mockCollector) Collect(_, _ string) {}

func getHandler(t *testing.T, opts ...testutil.GetDBOption) (http.Handler, *dependencies) {
	mdb, mdbURI := testutil.GetDB(t, databaseUri, config.SchemaMetadata, opts...)
	blockAdapter := testutil.GetBlockAdapter(t, &block.NoOpTranslator{})

	meta := index.NewDBIndex(mdb)

	adb, adbURI := testutil.GetDB(t, databaseUri, config.SchemaAuth, opts...)
	authService := auth.NewDBAuthService(adb, crypt.NewSecretStore([]byte("some secret")))

	migrator := db.NewDatabaseMigrator().
		AddDB(config.SchemaMetadata, mdbURI).
		AddDB(config.SchemaAuth, adbURI)

	server := api.NewServer(
		meta,
		blockAdapter,
		authService,
		&mockCollector{},
		migrator,
	)

	handler, err := server.Handler()
	if err != nil {
		t.Fatal(err)
	}
	return handler, &dependencies{
		blocks: blockAdapter,
		auth:   authService,
		meta:   meta,
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
