package pyramid

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/block/mem"
	"github.com/treeverse/lakefs/pyramid/params"
)

const (
	blockStoragePrefix = "prefix"
	allocatedDiskBytes = 4 * 1024 * 1024
)

func TestSimpleWriteRead(t *testing.T) {
	namespace := uuid.New().String()
	filename := "1/2/file1.txt"

	content := []byte("hello world!")
	writeToFile(t, namespace, filename, content)
	checkContent(t, namespace, filename, content)
}

func TestReadFailDuringWrite(t *testing.T) {
	namespace := uuid.New().String()
	filename := "file1"
	f, err := fs.Create(namespace)
	require.NoError(t, err)

	content := []byte("some content")
	n, err := f.Write(content)
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	readF, err := fs.Open(namespace, filename)
	require.Nil(t, readF)
	require.Error(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, f.Store(filename))
	checkContent(t, namespace, filename, content)
}

func TestEvictionSingleNamespace(t *testing.T) {
	testEviction(t, uuid.New().String())
}

func TestEvictionMultipleNamespaces(t *testing.T) {
	testEviction(t, uuid.New().String(),
		uuid.New().String(),
		uuid.New().String())
}

func TestStartup(t *testing.T) {
	fsName := uuid.New().String()
	namespace := uuid.New().String()

	// cleanup
	baseDir := path.Join(os.TempDir(), fsName)
	defer func() {
		if err := os.RemoveAll(baseDir); err != nil {
			t.Fatal("Remove all filed under", baseDir, err)
		}
	}()

	namespacePath := path.Join(baseDir, namespace)
	workspacePath := path.Join(namespacePath, workspaceDir)
	if err := os.MkdirAll(workspacePath, os.ModePerm); err != nil {
		t.Fatal("make dir under", workspacePath, err)
	}

	filename := "ThisShouldStay"
	content := []byte("This Should Stay - I'm telling You!!!!")
	if err := ioutil.WriteFile(path.Join(namespacePath, filename), content, os.ModePerm); err != nil {
		t.Fatal("write file", filename, err)
	}

	if err := ioutil.WriteFile(path.Join(workspacePath, "ThisShouldNotStay"), []byte("ThisShouldNotStay"), os.ModePerm); err != nil {
		t.Fatal("write file", err)
	}

	localFS, err := NewFS(&params.Params{
		FSName:             fsName,
		Adaptor:            mem.New(),
		BlockStoragePrefix: blockStoragePrefix,
		Local: params.LocalDiskParams{
			BaseDir: os.TempDir(),
		},
		AllocatedDiskBytes: allocatedDiskBytes,
	})
	if err != nil {
		t.Fatal("NewFS", err)
	}

	dir, err := os.Open(workspacePath)
	require.Nil(t, dir)
	require.True(t, os.IsNotExist(err))

	f, err := localFS.Open(namespace, filename)
	defer f.Close()
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, content, bytes)
}

func testEviction(t *testing.T, namespaces ...string) {
	// making sure to fill the cache
	fileBytes := 512 * 1024
	numFiles := 5 * allocatedDiskBytes / fileBytes
	// write
	content := make([]byte, fileBytes)
	for i := 0; i < numFiles; i++ {
		filename := "file_" + strconv.Itoa(i)
		rand.Read(content)
		writeToFile(t, namespaces[i%len(namespaces)], filename, content)
	}

	// read
	for i := 0; i < numFiles; i++ {
		filename := "file_" + strconv.Itoa(i)

		f, err := fs.Open(namespaces[i%len(namespaces)], filename)
		require.NoError(t, err)

		_, err = ioutil.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
}

func TestInvalidArgs(t *testing.T) {
	f, err := fs.Create("not/a/valid/namespace")
	require.Nil(t, f)
	require.Error(t, err)
}

func TestMultipleConcurrentReads(t *testing.T) {
	var baseDir string
	fs, baseDir = createFSWithEviction(&mockEv{})

	defer func() { _ = os.RemoveAll(baseDir) }()

	// write a single file to lookup later
	namespace := uuid.New().String()
	filename := "1/2/file1.txt"
	content := []byte("hello world!")
	writeToFile(t, namespace, filename, content)

	// remove the file
	require.NoError(t, filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, filename) {
			return os.Remove(path)
		}
		return nil
	}))
	// try to read that file - only a single access to block storage is expected
	concurrencyLevel := 50
	adapter.wait = make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < concurrencyLevel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			checkContent(t, namespace, filename, content)
		}()
	}

	close(adapter.wait)
	wg.Wait()

	require.Equal(t, int64(1), adapter.GetCount())
}

func writeToFile(t *testing.T, namespace, filename string, content []byte) {
	t.Helper()
	f, err := fs.Create(namespace)
	require.NoError(t, err)

	n, err := f.Write(content)
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	require.NoError(t, f.Close())
	require.NoError(t, f.Store(filename))
}

func checkContent(t *testing.T, namespace string, filename string, content []byte) {
	t.Helper()
	f, err := fs.Open(namespace, filename)
	require.NoError(t, err)
	defer f.Close()

	bytes, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, content, bytes)
}

type mockEv struct{}

func (_ *mockEv) Touch(_ params.RelativePath) {}

func (_ *mockEv) Store(_ params.RelativePath, _ int64) bool {
	return true
}
