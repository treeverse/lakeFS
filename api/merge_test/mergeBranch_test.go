package mergeBranch

import (
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index/model"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/objects"

	"github.com/go-openapi/runtime"
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
	DefaultUserId = "example_user"
)

type dependencies struct {
	blocks block.Adapter
	auth   auth.Service
	meta   index.Index
	mpu    index.MultipartManager
}

func CreateRepo(t *testing.T, kv store.KVStore) {
	repoCreateDate, _ := time.Parse(testutil.TimeFormat, "Apr 7 15:13:13 2005 -0700")
	_, err := kv.RepoTransact("myrepo", func(ops store.RepoOperations) (i interface{}, e error) {
		repo := &model.Repo{
			RepoId:             "myrepo",
			CreationDate:       repoCreateDate.Unix(),
			DefaultBranch:      index.DefaultBranch,
			PartialCommitRatio: 1,
		}
		err := ops.WriteRepo(repo)
		if err != nil {
			t.Fatal(err)
		}
		commit := &model.Commit{
			Tree:      ident.Empty(),
			Parents:   []string{},
			Timestamp: repoCreateDate.Unix(),
			Metadata:  make(map[string]string),
		}
		commitId := ident.Hash(commit)
		commit.Address = commitId
		err = ops.WriteCommit(commitId, commit)
		if err != nil {
			return nil, err
		}
		err = ops.WriteBranch(repo.GetDefaultBranch(), &model.Branch{
			Name:          repo.GetDefaultBranch(),
			Commit:        commitId,
			CommitRoot:    commit.GetTree(),
			WorkspaceRoot: commit.GetTree(),
		})
		return repo, nil
	})
	if err != nil {
		t.Fatal(err)
	}
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
	CreateRepo(t, store.KVStore(*indexStore))
	authService := auth.NewKVAuthService(db)

	server := api.NewServer(
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

func TestMergeDiff(t *testing.T) {
	handler, deps, close := getHandler(t)
	defer close()
	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	err := uploadObject(t, "t/v/s", "master", "myrepo", 1024, clt, creds)
	if err != nil {
		t.Fatal("failed creating object t/v/s  ", err, "\n")
	}
	err = createBranch(t, "br-1", "master", "myrepo", clt, creds)
	if err != nil {
		t.Fatal("failed creating branch br-1 ", err, "\n")
	}
	err = uploadObject(t, "t/v/s1", "master", "myrepo", 2048, clt, creds)
	if err != nil {
		t.Fatal("failed creating object t/v/s  ", err, "\n")
	}
	err = deps.meta.Merge("myrepo", "master", "br-1")
}

func createBranch(t *testing.T, name, parent, repo string, clt *client.Lakefs, creds *authmodel.APICredentials) error {
	createBranchParams := branches.NewCreateBranchParams()
	var b models.BranchCreation
	b.ID = &name
	b.SourceRefID = &parent
	createBranchParams.Branch = &b
	createBranchParams.RepositoryID = repo
	branchResp, err := clt.Branches.CreateBranch(createBranchParams, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))
	t.Log(err)
	t.Log(branchResp.Payload.CommitID)
	t.Log(branchResp.Payload.ID)
	return err
}

func uploadObject(t *testing.T, path, branch, repo string, size int64, clt *client.Lakefs, creds *authmodel.APICredentials) error {
	uploadObjectParams := objects.NewUploadObjectParams()
	uploadObjectParams.BranchID = "master"
	uploadObjectParams.Content = NewReader(size, "content")
	uploadObjectParams.RepositoryID = "myrepo"
	uploadObjectParams.Path = "t/v/s"
	resp, err := clt.Objects.UploadObject(uploadObjectParams, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))
	if err != nil {
		t.Fatalf("error uploading document\n")
	}
	t.Log(resp.Payload.Path)
	return err
}
