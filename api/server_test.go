package api_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/treeverse/lakefs/api/gen/models"

	"github.com/go-openapi/swag"

	httptransport "github.com/go-openapi/runtime/client"

	"github.com/treeverse/lakefs/api/gen/client/operations"

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
	clt := httptransport.NewWithClient("", "/", []string{"http"}, &http.Client{
		Transport: &roundTripper{r.Handler},
	})
	return clt.Submit(op)
}

func TestHandler_Basic(t *testing.T) {
	handler, deps, close := getHandler(t)
	defer close()
	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("valid auth", func(t *testing.T) {
		_, err := clt.Operations.ListRepositories(&operations.ListRepositoriesParams{}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))
		if err != nil {
			t.Fatalf("did not expect error when passing valid credentials")
		}
	})

	t.Run("invalid auth secret", func(t *testing.T) {
		_, err := clt.Operations.ListRepositories(&operations.ListRepositoriesParams{}, httptransport.BasicAuth(creds.AccessKeyId, "foobarbaz"))
		if err == nil {
			t.Fatalf("expect error when passing invalid credentials")
		}
		errMsg, ok := err.(*operations.ListRepositoriesDefault)
		if !ok {
			t.Fatal("expected default error answer")
		}

		if !strings.EqualFold(errMsg.GetPayload().Message, "authentication error") {
			t.Fatalf("expected authentication error, got error: %s", errMsg.GetPayload().Message)
		}
	})
}

func TestHandler_ListRepositoriesHandler(t *testing.T) {

	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("list no repos", func(t *testing.T) {
		resp, err := clt.Operations.ListRepositories(&operations.ListRepositoriesParams{},
			httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

		if err != nil {
			t.Fatal(err)
		}

		if resp.GetPayload() == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.GetPayload().Results) != 0 {
			t.Fatalf("expected 0 repositories, got %d", len(resp.GetPayload().Results))
		}
	})

	t.Run("list some repos", func(t *testing.T) {
		// write some repos
		testutil.Must(t, deps.meta.CreateRepo("foo1", "master"))
		testutil.Must(t, deps.meta.CreateRepo("foo2", "master"))
		testutil.Must(t, deps.meta.CreateRepo("foo3", "master"))

		resp, err := clt.Operations.ListRepositories(&operations.ListRepositoriesParams{},
			httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

		if err != nil {
			t.Fatal(err)
		}

		if resp.GetPayload() == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.GetPayload().Results) != 3 {
			t.Fatalf("expected 3 repositories, got %d", len(resp.GetPayload().Results))
		}
	})

	t.Run("paginate repos", func(t *testing.T) {
		// write some repos
		resp, err := clt.Operations.ListRepositories(&operations.ListRepositoriesParams{
			Amount: swag.Int64(2),
		}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

		if err != nil {
			t.Fatal(err)
		}

		if resp.GetPayload() == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.GetPayload().Results) != 2 {
			t.Fatalf("expected 3 repositories, got %d", len(resp.GetPayload().Results))
		}

		if !swag.BoolValue(resp.GetPayload().Pagination.HasMore) {
			t.Fatalf("expected more results from paginator, got none")
		}
	})

	t.Run("paginate repos after", func(t *testing.T) {
		// write some repos
		resp, err := clt.Operations.ListRepositories(&operations.ListRepositoriesParams{
			Amount: swag.Int64(2),
			After:  swag.String("foo2"),
		}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

		if err != nil {
			t.Fatal(err)
		}

		if resp.GetPayload() == nil {
			t.Fatalf("expected payload, got nil")
		}
		if len(resp.GetPayload().Results) != 1 {
			t.Fatalf("expected 1 repository, got %d", len(resp.GetPayload().Results))
		}

		if swag.BoolValue(resp.GetPayload().Pagination.HasMore) {
			t.Fatalf("expected no more results from paginator")
		}

		if !strings.EqualFold(resp.GetPayload().Results[0].ID, "foo3") {
			t.Fatalf("expected last pagination result to be foo3, got %s instead",
				resp.GetPayload().Results[0].ID)
		}
	})
}

func TestHandler_GetRepoHandler(t *testing.T) {

	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("get missing repo", func(t *testing.T) {
		_, err := clt.Operations.GetRepository(&operations.GetRepositoryParams{
			RepositoryID: "foo1",
		}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

		if err == nil {
			t.Fatalf("expected err calling missing repo")
		}

		if _, ok := err.(*operations.GetRepositoryNotFound); !ok {
			t.Fatalf("expected not found error getting missing repo")
		}
	})

	t.Run("get existing repo", func(t *testing.T) {
		deps.meta.CreateRepo("foo1", "some_non_default_branch")
		resp, err := clt.Operations.GetRepository(&operations.GetRepositoryParams{
			RepositoryID: "foo1",
		}, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))

		if err != nil {
			t.Fatalf("unexpected err calling get repo, %v", err)
		}

		if !strings.EqualFold(resp.GetPayload().DefaultBranch, "some_non_default_branch") {
			t.Fatalf("unexpected branch name %s, expected some_non_default_branch",
				resp.GetPayload().DefaultBranch)
		}
	})

}

func TestHandler_GetCommitHandler(t *testing.T) {

	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("get missing commit", func(t *testing.T) {
		_, err := clt.Operations.GetCommit(&operations.GetCommitParams{
			CommitID:     "b0a989d946dca26496b8280ca2bb0a96131a48b362e72f1789e498815992fffa",
			RepositoryID: "foo1",
		}, bauth)
		if err == nil {
			t.Fatalf("expected err calling missing commit")
		}

		if _, ok := err.(*operations.GetCommitNotFound); !ok {
			t.Fatalf("expected not found error getting missing commit")
		}
	})

	t.Run("get existing commit", func(t *testing.T) {
		deps.meta.CreateRepo("foo1", "master")
		deps.meta.Commit("foo1", "master", "some message", DefaultUserId, nil)
		b, err := deps.meta.GetBranch("foo1", "master")
		if err != nil {
			t.Fatal(err)
		}
		resp, err := clt.Operations.GetCommit(&operations.GetCommitParams{
			CommitID:     b.Commit,
			RepositoryID: "foo1",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected err calling commit, %v", err)
		}

		if !strings.EqualFold(resp.GetPayload().Committer, DefaultUserId) {
			t.Fatalf("unexpected commit id %s, expected %s",
				resp.GetPayload().Committer, DefaultUserId)
		}
	})
}

func TestHandler_CommitHandler(t *testing.T) {

	handler, deps, close := getHandler(t)
	defer close()

	// create user
	creds := createDefaultAdminUser(deps.auth, t)
	bauth := httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	t.Run("commit non-existent repo", func(t *testing.T) {
		_, err := clt.Operations.Commit(&operations.CommitParams{
			BranchID: "master",
			Commit: &models.CommitCreation{
				Message:  swag.String("some message"),
				Metadata: nil,
			},
			RepositoryID: "foo1",
		}, bauth)

		if err == nil {
			t.Fatalf("expected err calling missing repo for commit")
		}

		if _, ok := err.(*operations.CommitDefault); !ok {
			t.Fatalf("expected not found error when missing commit repo, got %v", err)
		}
	})

	t.Run("commit success", func(t *testing.T) {
		deps.meta.CreateRepo("foo1", "master")
		_, err := clt.Operations.Commit(&operations.CommitParams{
			BranchID: "master",
			Commit: &models.CommitCreation{
				Message:  swag.String("some message"),
				Metadata: nil,
			},
			RepositoryID: "foo1",
		}, bauth)

		if err != nil {
			t.Fatalf("unexpected err calling commit: %s", err.Error())
		}
	})
}

func TestHandler_CreateRepositoryHandler(t *testing.T) {

}

func TestHandler_DeleteRepositoryHandler(t *testing.T) {

}

func TestHandler_ListBranchesHandler(t *testing.T) {

}

func TestHandler_GetBranchHandler(t *testing.T) {

}

func TestHandler_CreateBranchHandler(t *testing.T) {

}

func TestHandler_DeleteBranchHandler(t *testing.T) {

}
