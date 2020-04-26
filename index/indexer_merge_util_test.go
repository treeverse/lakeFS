package index_test

import (
	"io"
	str "strings"
	"time"

	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/ident"
	"github.com/treeverse/lakefs/index"
	"github.com/treeverse/lakefs/index/model"
	pth "github.com/treeverse/lakefs/index/path"
	"github.com/treeverse/lakefs/testutil"
	"github.com/treeverse/lakefs/upload"

	"strconv"
	"testing"

	"crypto/rand"

	log "github.com/sirupsen/logrus"
)

const (
	DefaultUserId = 1
	REPO          = "example"
)

type dependencies struct {
	blocks block.Adapter
	meta   index.Index
}

func getDependencies(t *testing.T) *dependencies {
	mdb := testutil.GetDB(t, databaseUri, "lakefs_index")
	meta := index.NewDBIndex(mdb)
	blockAdapter := testutil.GetBlockAdapter(t)
	testutil.Must(t, meta.CreateRepo(REPO, "s3://"+REPO, "master"))
	return &dependencies{
		blocks: blockAdapter,
		meta:   meta,
	}
}

func testCommit(t *testing.T, index index.Index, branch, message string) *model.Commit {
	commit, err := index.Commit(REPO, branch, message, "", make(map[string]string))
	if err != nil {
		t.Fatal("could not commit", err)
	}
	return commit
}

func createBranch(t *testing.T, index index.Index, name, parent string) {
	_, err := index.CreateBranch(REPO, name, parent)
	if err != nil {
		t.Fatal("error creating branch", err)
	}
}

func uploadObject(t *testing.T, deps *dependencies, path, branch string, size int64) {
	blob, err := upload.ReadBlob(REPO, NewReader(size, "content"), deps.blocks, 1024*1024*64)
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
		RepositoryId: REPO,
		Name:         p.BaseName(),
		Address:      ident.Hash(obj),
		EntryType:    model.EntryTypeObject,
		CreationDate: writeTime,
		Size:         blob.Size,
		Checksum:     blob.Checksum,
	}
	err = deps.meta.WriteFile(REPO, branch, path, entry, obj)
	if err != nil {
		t.Error("error writing file", err)
		return
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
