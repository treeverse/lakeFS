package index_test

import (
	"bytes"
	"encoding/csv"
	"github.com/go-openapi/runtime"
	"github.com/treeverse/lakefs/api"
	"github.com/treeverse/lakefs/api/gen/client"
	"github.com/treeverse/lakefs/api/gen/client/commits"
	"github.com/treeverse/lakefs/api/gen/models"
	"github.com/treeverse/lakefs/auth"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
	"github.com/treeverse/lakefs/index/store"
	"github.com/treeverse/lakefs/permissions"
	"github.com/treeverse/lakefs/testutil"
	"io"
	"net/http"
	"net/http/httptest"
	str "strings"
	"time"

	"os"
	"strconv"
	"testing"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/treeverse/lakefs/api/gen/client/branches"
	"github.com/treeverse/lakefs/api/gen/client/objects"

	"crypto/rand"
	log "github.com/sirupsen/logrus"
	authmodel "github.com/treeverse/lakefs/auth/model"
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

func setupHelper(t *testing.T, deps *dependencies, handler http.Handler) (*authmodel.APICredentials, *client.Lakefs, *translationMap) {

	// create user
	creds := createDefaultAdminUser(deps.auth, t)

	// setup client
	clt := client.Default
	clt.SetTransport(&handlerTransport{Handler: handler})
	return creds, clt, newTranslationMap()

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
		i = "00000"[0:5-len(i)] + i
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
func showEntries(kv db.Store, ct *translationMap, cs csvStore, csvPrefix string) {
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
		cs.writeCSV(csvPrefix)
		cs.refresh()
		return nil, nil
	})
}
func testCommit(t *testing.T, branch, message string, clt *client.Lakefs, creds *authmodel.APICredentials) {
	repo := "myrepo"
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
func createBranch(t *testing.T, name, parent string, clt *client.Lakefs, creds *authmodel.APICredentials) {
	repo := "myrepo"
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

func uploadObject(t *testing.T, path, branch string, size int64, clt *client.Lakefs, creds *authmodel.APICredentials) {
	repo := "myrepo"
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

// CREATING SIMULATED CONTENT

type contentCreator struct {
	pos       int64
	maxLength int64
	name      string
}

func NewReader(max int64, name string) *contentCreator {
	var r *contentCreator
	r = new(contentCreator)
	r.maxLength = max
	r.name = name
	return r
}

const stepSize = 20

func (c *contentCreator) Name() string {
	return c.name
}

func (c *contentCreator) Read(b []byte) (int, error) {
	if c.pos < 0 {
		log.Panic("attempt to read from a closed content creator")
	}
	if c.pos == c.maxLength {
		return 0, io.EOF
	}
	retSize := minInt(int64(len(b)), int64(c.maxLength-c.pos))
	var nextCopyPos int64
	currentBlockNo := c.pos/stepSize + 1 // first block in number 1
	//create the first block, which may be the continuation of a previous block
	remaining := (c.pos % stepSize)
	if remaining != 0 || retSize < stepSize {
		// the previous read did not end on integral stepSize boundry, or the retSize
		// is less than a block
		copiedSize := int64(copy(b, makeBlock(currentBlockNo)[remaining:minInt(stepSize, remaining+retSize)]))
		nextCopyPos = copiedSize
		currentBlockNo++
	}
	// create the blocks between first and last. Those are always full
	fullBlocksNo := (retSize - nextCopyPos) / stepSize
	for i := int64(0); i < fullBlocksNo; i++ {
		copy(b[nextCopyPos:nextCopyPos+stepSize], makeBlock(currentBlockNo))
		currentBlockNo++
		nextCopyPos += stepSize
	}
	// create the trailing block (if needed)
	remainingBytes := (c.pos + retSize) % stepSize
	if remainingBytes != 0 && (c.pos%stepSize+retSize) > stepSize {
		// destination will be smaller than the returned block, copy size is the minimum size of dest and source
		copy(b[nextCopyPos:nextCopyPos+remainingBytes], makeBlock(currentBlockNo)) // destination will be smaller than step size
	}
	c.pos += retSize
	if c.pos == c.maxLength {
		return int(retSize), io.EOF
	} else {
		if c.pos < c.maxLength {
			return int(retSize), nil
		} else {
			log.Panic("reader programming error - got past maxLength")
			return int(retSize), nil
		}
	}

}

func makeBlock(n int64) string {
	if n == 1 { // make first block random
		b := make([]byte, stepSize)
		rand.Read(b)
		return string(b)
	}
	f := strconv.Itoa(int(n * stepSize))
	block := str.Repeat("*", stepSize-len(f)) + f
	return block
}

func (c *contentCreator) Close() error {
	c.pos = -1
	return nil
}

func minInt(i1, i2 int64) int64 {
	if i1 < i2 {
		return i1
	} else {
		return i2
	}
}

func (c *contentCreator) Seek(seekPoint int64) error {
	if c.pos < 0 {
		log.Panic("attempt to read from a closed content creator")
	}
	if seekPoint >= c.maxLength {
		return io.EOF
	}
	c.pos = seekPoint
	return nil
}

func getObject(t *testing.T, i index.Index, repo, branch, path string, Expected bool, message string) bool {
	_, err := i.ReadEntryObject(repo, branch, path)
	if Expected != (err == nil) {
		t.Error(message, err)
	}
	return (err == nil)
}
