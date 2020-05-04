package index_test

import (
	"strings"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
	pth "github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/testutil"
	"github.com/treeverse/lakefs/upload"

	"testing"
)

const (
	TestRepo = "example"
)

type dependencies struct {
	blocks block.Adapter
	meta   index.Index
}

func getDependencies(t *testing.T) *dependencies {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	meta := index.NewDBIndex(mdb)
	blockAdapter := testutil.GetBlockAdapter(t)
	testutil.Must(t, meta.CreateRepo(TestRepo, "s3://"+TestRepo, "master"))
	return &dependencies{
		blocks: blockAdapter,
		meta:   meta,
	}
}

func testCommit(t *testing.T, index index.Index, branch, message string) *model.Commit {
	commit, err := index.Commit(TestRepo, branch, message, "", make(map[string]string))
	if err != nil {
		t.Fatal("could not commit", err)
	}
	return commit
}

func createBranch(t *testing.T, index index.Index, name, parent string) {
	_, err := index.CreateBranch(TestRepo, name, parent)
	if err != nil {
		t.Fatal("error creating branch", err)
	}
}

func uploadObject(t *testing.T, deps *dependencies, path, branch string, content string) {
	blob, err := upload.ReadBlob(TestRepo, strings.NewReader(content), deps.blocks, 1024*64)
	if err != nil {
		t.Error("error storing object in blocks", err)
		return
	}
	obj := &model.Object{
		Blocks:   blob.Blocks,
		Checksum: blob.Checksum,
		Size:     blob.Size,
	}
	p := pth.New(path, model.EntryTypeObject)
	writeTime := time.Now()
	entry := &model.Entry{
		RepositoryId: TestRepo,
		Name:         p.BaseName(),
		Address:      ident.Hash(obj),
		EntryType:    model.EntryTypeObject,
		CreationDate: writeTime,
		Size:         blob.Size,
		Checksum:     blob.Checksum,
	}
	err = deps.meta.WriteFile(TestRepo, branch, path, entry, obj)
	if err != nil {
		t.Error("error writing file", err)
		return
	}
}

func getObject(t *testing.T, idx index.Index, repo, branch, path string, expectedErr bool, message string) {
	_, err := idx.ReadEntryObject(repo, branch, path)
	if expectedErr != (err == nil) {
		t.Error(message, err)
	}
}
