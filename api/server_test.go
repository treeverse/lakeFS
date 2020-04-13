package api_test

import (
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/index/store"

	"github.com/ory/dockertest/v3"

	httptransport "github.com/go-openapi/runtime/client"

	"github.com/treeverse/lakefs/api/gen/client/repositories"

	"github.com/go-openapi/runtime"
	"github.com/treeverse/lakefs/api/gen/client"

	"github.com/treeverse/lakefs/permissions"

	authmodel "github.com/treeverse/lakefs/auth/model"

	"github.com/treeverse/lakefs/block"

	"github.com/treeverse/lakefs/auth"

	"github.com/treeverse/lakefs/index"

	"github.com/treeverse/lakefs/testutil"

	"github.com/treeverse/lakefs/api"
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
	mpu    index.MultipartManager
}

func createDefaultAdminUser(authService auth.Service, t *testing.T) *authmodel.Credential {
	// create user
	user := &authmodel.User{
		Email:    "admin@example.com",
		FullName: "admin user",
	}
	testutil.Must(t, authService.CreateUser(user))

	// create role
	role := &authmodel.Role{
		DisplayName: "Admins",
	}
	testutil.Must(t, authService.CreateRole(role))

	// attach policies
	policies := []*authmodel.Policy{
		{
			Permission: string(permissions.ManageRepos),
			Arn:        "arn:treeverse:repos:::*",
		},
		{
			Permission: string(permissions.ReadRepo),
			Arn:        "arn:treeverse:repos:::*",
		},
		{
			Permission: string(permissions.WriteRepo),
			Arn:        "arn:treeverse:repos:::*",
		},
	}
	for _, policy := range policies {
		testutil.Must(t, authService.AssignPolicyToRole(role.Id, policy))
	}

	// assign user to role
	testutil.Must(t, authService.AssignRoleToUser(role.Id, user.Id))

	creds, err := authService.CreateUserCredentials(user)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

func getHandler(t *testing.T) (http.Handler, *dependencies) {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	blockAdapter := testutil.GetBlockAdapter(t)

	meta := index.NewDBIndex(mdb)
	mpu := index.NewDBMultipartManager(store.NewDBStore(mdb))

	adb := testutil.GetDB(t, databaseUri, "lakefs_auth")
	authService := auth.NewDBAuthService(adb, "some secret")

	server := api.NewServer(
		meta,
		mpu,
		blockAdapter,
		authService,
	)

	srv, err := server.SetupServer()
	if err != nil {
		t.Fatal(err)
	}
	return srv.GetHandler(), &dependencies{
		blocks: blockAdapter,
		auth:   authService,
		meta:   meta,
		mpu:    mpu,
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
