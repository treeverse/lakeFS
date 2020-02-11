package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	httptransport "github.com/go-openapi/runtime/client"

	"github.com/treeverse/lakefs/api/gen/client/repositories"

	"github.com/go-openapi/runtime"
	"github.com/treeverse/lakefs/api/gen/client"

	"github.com/treeverse/lakefs/permissions"

	authmodel "github.com/treeverse/lakefs/auth/model"

	"github.com/treeverse/lakefs/block"

	"github.com/treeverse/lakefs/index/store"

	"github.com/treeverse/lakefs/auth"

	"github.com/treeverse/lakefs/index"

	"github.com/treeverse/lakefs/testutil"

	"github.com/treeverse/lakefs/api"
)

const (
	TestRegion    = "us-east-1"
	DefaultUserId = "example_user"
)

type dependencies struct {
	blocks block.Adapter
	auth   auth.Service
	meta   index.Index
	mpu    index.MultipartManager
}

func createDefaultAdminUser(authService auth.Service, t *testing.T) *authmodel.APICredentials {
	testutil.Must(t, authService.CreateRole(&authmodel.Role{
		Id:   "admin",
		Name: "admin",
		Policies: []*authmodel.Policy{
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
		},
	}))

	user := &authmodel.User{
		Id:    DefaultUserId,
		Roles: []string{"admin"},
	}
	testutil.Must(t, authService.CreateUser(user))
	creds, err := authService.CreateUserCredentials(user)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

func getHandler(t *testing.T) (http.Handler, *dependencies, func()) {
	db, dbCloser := testutil.GetDB(t)
	blockAdapter, fsCloser := testutil.GetBlockAdapter(t)
	indexStore := store.NewKVStore(db)
	meta := index.NewKVIndex(indexStore)
	mpu := index.NewKVMultipartManager(indexStore)

	authService := auth.NewKVAuthService(db)

	server := api.NewServer(
		TestRegion,
		meta,
		mpu,
		blockAdapter,
		authService,
	)

	closer := func() {
		dbCloser()
		fsCloser()
	}

	srv, err := server.SetupServer()
	if err != nil {
		closer()
		t.Fatal(err)
	}
	return srv.GetHandler(), &dependencies{
		blocks: blockAdapter,
		auth:   authService,
		meta:   meta,
		mpu:    mpu,
	}, closer
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
	handler, deps, close := getHandler(t)
	defer close()
	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("valid auth", func(t *testing.T) {
		_, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))
		if err != nil {
			t.Fatalf("did not expect error when passing valid credentials")
		}
	})

	t.Run("invalid auth secret", func(t *testing.T) {
		_, err := clt.Repositories.ListRepositories(&repositories.ListRepositoriesParams{}, httptransport.BasicAuth(creds.AccessKeyId, "foobarbaz"))
		if err == nil {
			t.Fatalf("expect error when passing invalid credentials")
		}
		errMsg, ok := err.(*repositories.ListRepositoriesDefault)
		if !ok {
			t.Fatal("expected default error answer")
		}

		if !strings.EqualFold(errMsg.GetPayload().Message, "authentication error") {
			t.Fatalf("expected authentication error, got error: %s", errMsg.GetPayload().Message)
		}
	})
}
