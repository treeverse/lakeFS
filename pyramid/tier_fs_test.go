package pyramid

import (
	"bytes"
	"context"
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
	ctx := context.Background()
	namespace := uuid.New().String()
	filename := "1/2/file1.txt"

	content := []byte("hello world!")
	writeToFile(t, ctx, namespace, filename, content)
	checkContent(t, ctx, namespace, filename, content)
}

func TestReadFailDuringWrite(t *testing.T) {
	ctx := context.Background()
	namespace := uuid.New().String()
	filename := "file1"
	f, err := fs.Create(ctx, namespace)
	require.NoError(t, err)

	content := []byte("some content")
	n, err := f.Write(content)
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	readF, err := fs.Open(ctx, namespace, filename)
	require.Nil(t, readF)
	require.Error(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, f.Store(ctx, filename))
	checkContent(t, ctx, namespace, filename, content)
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
	ctx := context.Background()
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

	localFS, err := NewFS(&params.InstanceParams{
		FSName:              fsName,
		DiskAllocProportion: 1.0,
		SharedParams: params.SharedParams{
			Adapter:            mem.New(),
			BlockStoragePrefix: blockStoragePrefix,
			Local: params.LocalDiskParams{
				BaseDir:             os.TempDir(),
				TotalAllocatedBytes: allocatedDiskBytes,
			},
		},
	})
	if err != nil {
		t.Fatal("NewFS", err)
	}

	dir, err := os.Open(workspacePath)
	require.Nil(t, dir)
	require.True(t, os.IsNotExist(err))

	f, err := localFS.Open(ctx, namespace, filename)
	defer func() { _ = f.Close() }()
	require.NoError(t, err)

	data, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, content, data)
}

func testEviction(t *testing.T, namespaces ...string) {
	ctx := context.Background()
	// making sure to fill the cache
	fileBytes := 512 * 1024
	numFiles := 5 * allocatedDiskBytes / fileBytes
	// write
	content := make([]byte, fileBytes)
	for i := 0; i < numFiles; i++ {
		filename := "file_" + strconv.Itoa(i)
		rand.Read(content)
		writeToFile(t, ctx, namespaces[i%len(namespaces)], filename, content)
	}

	// read
	for i := 0; i < numFiles; i++ {
		filename := "file_" + strconv.Itoa(i)

		f, err := fs.Open(ctx, namespaces[i%len(namespaces)], filename)
		require.NoError(t, err)

		_, err = ioutil.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
}

func TestMultipleConcurrentReads(t *testing.T) {
	ctx := context.Background()
	var baseDir string
	fs, baseDir = createFSWithEviction(&mockEv{})

	defer func() { _ = os.RemoveAll(baseDir) }()

	// write a single file to lookup later
	namespace := uuid.New().String()
	filename := "1/2/file1.txt"
	content := []byte("hello world!")
	writeToFile(t, ctx, namespace, filename, content)

	// remove the file
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, filename) {
			return os.Remove(path)
		}
		return nil
	})
	require.NoError(t, err)
	// try to read that file - only a single access to block storage is expected
	const concurrencyLevel = 50
	adapter.wait = make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)
	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			defer wg.Done()
			checkContent(t, ctx, namespace, filename, content)
		}()
	}

	close(adapter.wait)
	wg.Wait()

	require.Equal(t, int64(1), adapter.GetCount())
}

func writeToFile(t *testing.T, ctx context.Context, namespace, filename string, content []byte) {
	t.Helper()
	f, err := fs.Create(ctx, namespace)
	require.NoError(t, err)

	n, err := f.Write(content)
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	require.NoError(t, f.Close())
	require.NoError(t, f.Store(ctx, filename))
}

func checkContent(t *testing.T, ctx context.Context, namespace string, filename string, content []byte) {
	t.Helper()
	f, err := fs.Open(ctx, namespace, filename)
	if err != nil {
		t.Errorf("Failed to open namespace:%s filename:%s - %s", namespace, filename, err)
		return
	}
	defer f.Close()

	data, err := ioutil.ReadAll(f)
	if err != nil {
		t.Errorf("Failed to read all namespace:%s filename:%s - %s", namespace, filename, err)
		return
	}
	if !bytes.Equal(content, data) {
		t.Errorf("Content mismatch reading namespace:%s filename:%s", namespace, filename)
	}
}

type mockEv struct{}

func (*mockEv) Touch(params.RelativePath) {}

func (*mockEv) Store(params.RelativePath, int64) bool {
	return true
}
