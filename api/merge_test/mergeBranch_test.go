package mergeBranch_test

import (
	"bytes"
	"encoding/csv"
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index/model"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
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
	db     db.Store
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
		db:     db,
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

var reportedTypes = []string{"entries", "branches"}

func TestMergeDiff(t *testing.T) {
	chacksumTranslat := newTranslationMap()
	cs := make(csvStore)
	cs.addType("branches", []string{"name", "commit", "commitRoot", "workspaceRoot"})
	cs.addType("entries", []string{"owner", "name", "address", "type", "size", "checksum"})
	cs.addType("commits", []string{"address", "tree"})
	handler, deps, close := getHandler(t)
	defer close()
	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})

	uploadObject(t, "t/v/s", "master", "myrepo", 1024, clt, creds)
	commit(t, "myrepo", "master", "master-1", clt, creds)
	createBranch(t, "br-1", "master", "myrepo", clt, creds)
	uploadObject(t, "t/v1/s", "master", "myrepo", 10000, clt, creds)
	//uploadObject(t, "t/v1/s1", "master", "myrepo", 5000, clt, creds)
	//uploadObject(t, "t/v/s1", "master", "myrepo", 2048, clt, creds)
	uploadObject(t, "t/v/s31", "br-1", "myrepo", 20480, clt, creds)
	/*uploadObject(t, "t/v/s1", "br-1", "myrepo", 4096, clt, creds)
	uploadObject(t, "t/v1/s1", "br-1", "myrepo", 1024, clt, creds)
	uploadObject(t, "t/v1/s1", "master", "myrepo", 2048, clt, creds)
	uploadObject(t, "t/v/s2", "br-1", "myrepo", 5000, clt, creds)
	commit(t,"myrepo","master","master-2",clt,creds)
	commit(t,"myrepo","br-1","br-1-2",clt,creds)
	uploadObject(t, "t/v/s2", "master", "myrepo", 5000, clt, creds)
	uploadObject(t, "t/v/s3", "br-1", "myrepo", 5000, clt, creds)
	uploadObject(t, "t/v1/s3", "br-1", "myrepo", 3000, clt, creds)*/
	commit(t, "myrepo", "master", "master-2", clt, creds)
	commit(t, "myrepo", "br-1", "master-2", clt, creds)
	//commit(t,"myrepo","br-1","br-1-2",clt,creds)
	//uploadObject(t, "t/v1/s4", "master", "myrepo", 3000, clt, creds)
	//uploadObject(t, "t/v1/s4", "br-1", "myrepo", 3000, clt, creds)
	showEntries(db.Store(deps.db), chacksumTranslat, cs)
	cs.writeCSV("1-")

	_ = deps.meta.Merge("myrepo", "master", "br-1")
}

type translationMap struct {
	count int
	table map[string]string
}

func newTranslationMap() *translationMap {
	p := make(map[string]string)
	return &translationMap{0, p}
}

func (t *translationMap) getId(s string) string {
	val, stat := t.table[s]
	if stat {
		return val
	} else {
		t.count++
		i := strconv.Itoa(t.count)
		i = "000"[0:3-len(i)] + i
		t.table[s] = i
		return t.table[s]
	}
}

type csvStore map[string][][]string

func (cs csvStore) addType(t string, l []string) {
	cs[t] = make([][]string, 0)
	cs.add(t, l)
}

func (cs csvStore) add(t string, a []string) {
	cs[t] = append(cs[t], a)
}

func (cs csvStore) refresh() {
	for k, v := range cs {
		cs[k] = v[0:1]
	}
}

func (cs csvStore) writeCSV(prefix string) {
	for k, v := range cs {
		f, err := os.Create(prefix + k + ".csv")
		if err != nil {
			panic(err)
		}
		o := csv.NewWriter(f)
		err = o.WriteAll(v)
		if err != nil {
			panic(err)
		}
		f.Close()

	}
}
func showEntries(kv db.Store, ct *translationMap, cs csvStore) {
	kv.ReadTransact(func(q db.ReadQuery) (i interface{}, err error) {
		iter, closer := q.RangeAll()
		defer closer()
		/* func (m *Entry) XXX_Unmarshal(b []byte) error {
		return xxx_messageInfo_Entry.Unmarshal(m, b) */
		for iter.Advance() {
			item, _ := iter.Get()
			//fmt.Print(len(item.Key),"   ",len(item.Value),"  ")
			k := db.KeyFromBytes(item.Key)
			if bytes.Compare(k[0], []byte("entries")) == 0 {
				nk := []string{ct.getId(string(k[2])), string(k[3])}
				m := new(model.Entry)
				m.XXX_Unmarshal(item.Value)
				nk = append(nk, ct.getId(m.Address))
				nk = append(nk, strconv.Itoa(int(m.Type)))
				nk = append(nk, strconv.Itoa(int(m.Size)))
				nk = append(nk, ct.getId(m.Checksum))
				cs.add("entries", nk)
			} else if bytes.Compare(k[0], []byte("branches")) == 0 {

				nk := []string{string(k[2])}
				m := new(model.Branch)
				m.XXX_Unmarshal(item.Value)
				nk = append(nk, ct.getId(m.Commit))
				nk = append(nk, ct.getId(m.CommitRoot))
				nk = append(nk, ct.getId(m.WorkspaceRoot))
				cs.add("branches", nk)
			} else if bytes.Compare(k[0], []byte("commits")) == 0 {
				nk := []string{}
				m := new(model.Commit)
				m.XXX_Unmarshal(item.Value)
				nk = append(nk, ct.getId(m.Address))
				nk = append(nk, ct.getId(m.Tree))
				cs.add("commits", nk)
			}
		}
		return nil, nil
	})
}
func commit(t *testing.T, repo, branch, message string, clt *client.Lakefs, creds *authmodel.APICredentials) {
	commitParams := commits.NewCommitParams()
	commitParams.BranchID = branch
	commitParams.RepositoryID = repo
	commitParams.Commit = new(models.CommitCreation)
	commitParams.Commit.Message = &message
	commitParams.Commit.Metadata = make(map[string]string)
	_, err := clt.Commits.Commit(commitParams, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))
	if err != nil {
		t.Fatal("could not commit\n")
	}
}
func createBranch(t *testing.T, name, parent, repo string, clt *client.Lakefs, creds *authmodel.APICredentials) {
	createBranchParams := branches.NewCreateBranchParams()
	var b models.BranchCreation
	b.ID = &name
	b.SourceRefID = &parent
	createBranchParams.Branch = &b
	createBranchParams.RepositoryID = repo
	_, err := clt.Branches.CreateBranch(createBranchParams, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))
	if err != nil {
		t.Fatal("error creating brancht\n")
	}
}

func uploadObject(t *testing.T, path, branch, repo string, size int64, clt *client.Lakefs, creds *authmodel.APICredentials) {
	uploadObjectParams := objects.NewUploadObjectParams()
	uploadObjectParams.BranchID = branch
	uploadObjectParams.Content = NewReader(size, "content")
	uploadObjectParams.RepositoryID = repo
	uploadObjectParams.Path = path
	_, err := clt.Objects.UploadObject(uploadObjectParams, httptransport.BasicAuth(creds.AccessKeyId, creds.AccessSecretKey))
	if err != nil {
		t.Fatal("error uploading document\n")
	}
}
