package pyramid

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/block/mem"
	"github.com/treeverse/lakefs/pkg/cache"
	"github.com/treeverse/lakefs/pkg/config"
	"github.com/treeverse/lakefs/pkg/logging"
	"github.com/treeverse/lakefs/pkg/pyramid/params"
)

const (
	blockStoragePrefix = "prefix"
	allocatedDiskBytes = 4 * 1024 * 1024
	secondaryStorageID = "another_one"
)

func TestSimpleWriteRead(t *testing.T) {
	ctx := context.Background()
	namespace := uniqueNamespace()
	filename := "1/2/file1.txt"

	content := []byte("hello world!")
	writeToFile(t, ctx, config.SingleBlockstoreID, namespace, filename, content)
	err := checkContent(t, ctx, config.SingleBlockstoreID, namespace, filename, content)
	require.NoError(t, err)
}

func TestReadFailDuringWrite(t *testing.T) {
	ctx := context.Background()
	namespace := uniqueNamespace()
	filename := "file1"
	f, err := fs.Create(ctx, "", namespace)
	require.NoError(t, err)

	content := []byte("some content")
	n, err := f.Write(content)
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	readF, err := fs.Open(ctx, "", namespace, filename)
	require.Nil(t, readF)
	require.Error(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, f.Store(ctx, filename))
	err = checkContent(t, ctx, config.SingleBlockstoreID, namespace, filename, content)
	require.NoError(t, err)
}

func TestOneWriteTwoStorageIDs(t *testing.T) {
	ctx := context.Background()
	namespace := uniqueNamespace()
	filename := "1/2/file1.txt"
	content := []byte("hello world!")

	// Write content to default SID
	writeToFile(t, ctx, config.SingleBlockstoreID, namespace, filename, content)

	// Read it from a different SID: should fail!
	_, err := fs.Open(ctx, secondaryStorageID, namespace, filename)
	require.ErrorContains(t, err, "not found")
}

func TestTwoWritesTwoStorageIDs(t *testing.T) {
	ctx := context.Background()
	namespace := uniqueNamespace()
	filename := "1/2/file1.txt"
	content1 := []byte("hello world!")
	content2 := []byte("goodbye world!")

	// Write content to two
	writeToFile(t, ctx, config.SingleBlockstoreID, namespace, filename, content1)
	writeToFile(t, ctx, secondaryStorageID, namespace, filename, content2)

	// Check that both writes succeed
	err := checkContent(t, ctx, config.SingleBlockstoreID, namespace, filename, content1)
	require.NoError(t, err)
	err = checkContent(t, ctx, secondaryStorageID, namespace, filename, content2)
	require.NoError(t, err)
}

func TestEvictionSingleNamespace(t *testing.T) {
	testEviction(t, uniqueNamespace())
}

func uniqueNamespace() string {
	return "mem://" + uuid.New().String()
}

func TestEvictionMultipleNamespaces(t *testing.T) {
	testEviction(t,
		uniqueNamespace(),
		uniqueNamespace(),
		uniqueNamespace(),
	)
}

func TestStartup(t *testing.T) {
	ctx := context.Background()
	fsName := uniqueNamespace()
	// cleanup
	baseDir := path.Join(os.TempDir(), fsName)
	defer func() {
		if t.Failed() {
			// Leave behind the evidence.
			return
		}
		if err := os.RemoveAll(baseDir); err != nil {
			t.Fatal("Remove all files under", baseDir, err)
		}
	}()

	namespaceID := uuid.New().String()
	uniquePath := path.Join(baseDir, namespaceID)
	workspacePath := path.Join(uniquePath, workspaceDir)
	if err := os.MkdirAll(workspacePath, os.ModePerm); err != nil {
		t.Fatal("make dir under", workspacePath, err)
	}

	filename := "ThisShouldStay"
	content := []byte("This Should Stay - I'm telling You!!!!")
	if err := os.WriteFile(path.Join(uniquePath, filename), content, os.ModePerm); err != nil {
		t.Fatal("write file", filename, err)
	}

	if err := os.WriteFile(path.Join(workspacePath, "ThisShouldNotStay"), []byte("ThisShouldNotStay"), os.ModePerm); err != nil {
		t.Fatal("write file", err)
	}

	localFS, err := NewFS(&params.InstanceParams{
		FSName:              fsName,
		DiskAllocProportion: 1.0,
		SharedParams: params.SharedParams{
			Logger:             logging.ContextUnavailable(),
			Adapter:            mem.New(context.Background()),
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
	assert.Nil(t, dir, "expected to fail to open %s", workspacePath)
	// os.IsNotExist does not look as hard to errors.Is; for errors returned directly from
	// package os this does not matter.
	assert.Error(t, err, os.ErrNotExist, "expected %s not to exist", workspacePath)

	f, err := localFS.Open(ctx, "", "mem://"+namespaceID, filename)
	defer func() { _ = f.Close() }()
	assert.NoError(t, err)

	data, err := io.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, content, data)
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
		_, err := rand.Read(content)
		if err != nil {
			t.Fatal("rand.Read", err)
		}
		writeToFile(t, ctx, config.SingleBlockstoreID, namespaces[i%len(namespaces)], filename, content)
	}

	// read
	for i := 0; i < numFiles; i++ {
		filename := "file_" + strconv.Itoa(i)

		f, err := fs.Open(ctx, "", namespaces[i%len(namespaces)], filename)
		require.NoError(t, err)

		_, err = io.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}
}

func TestMultipleConcurrentReads(t *testing.T) {
	ctx := context.Background()
	var baseDir string
	fs, baseDir = createFSWithEviction(&mockEv{})

	defer func() { _ = os.RemoveAll(baseDir) }()

	// write a single file to look up later
	namespace := uniqueNamespace()
	filename := "1/2/file1.txt"
	content := []byte("hello world!")
	writeToFile(t, ctx, config.SingleBlockstoreID, namespace, filename, content)

	// remove the file
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if strings.HasSuffix(path, filename) {
			return os.Remove(path)
		}
		return nil
	})
	require.NoError(t, err)
	// try to read that file - only single access to block storage is expected
	const concurrencyLevel = 50
	adapter.wait = make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)
	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			defer wg.Done()
			_ = checkContent(t, ctx, config.SingleBlockstoreID, namespace, filename, content)
		}()
	}

	close(adapter.wait)
	wg.Wait()

	require.Equal(t, int64(1), adapter.GetCount())
}

func TestRemoveTempFileOnStoreError(t *testing.T) {
	ctx := context.Background()

	// Create a temporary directory for testing
	tempDir := t.TempDir()
	fsName := uniqueNamespace()
	baseDir := path.Join(tempDir, fsName)

	// Create TierFS with failing adapter
	failingAdapter := &mockFailingAdapter{Adapter: nil}
	tfs := &TierFS{
		logger:         logging.ContextUnavailable(),
		adapter:        failingAdapter,
		fsLocalBaseDir: baseDir,
		fsName:         fsName,
		syncDir:        &directory{ceilingDir: baseDir},
		keyLock:        cache.NewChanOnlyOne(),
		remotePrefix:   blockStoragePrefix,
	}

	// Create eviction control that accepts all files
	tfs.eviction = &mockEv{}

	namespace := uniqueNamespace()
	filename := "test_file.txt"
	content := []byte("test content")

	// Create a file using TierFS.Create()
	storedFile, err := tfs.Create(ctx, config.SingleBlockstoreID, namespace)
	require.NoError(t, err)

	// Write content to the file
	n, err := storedFile.Write(content)
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	// Close the file
	require.NoError(t, storedFile.Close())

	// Get the actual temp file path from the WRFile
	wrFile, ok := storedFile.(*WRFile)
	require.True(t, ok, "storedFile should be *WRFile")
	tempPath := wrFile.File.Name()

	// Verify the temp file exists before calling Store()
	_, err = os.Stat(tempPath)
	require.NoError(t, err, "temp file should exist before Store()")

	// Call Store() - this should fail due to the mock adapter and trigger removeTempFile()
	err = storedFile.Store(ctx, filename)
	require.Error(t, err, "Store() should fail due to mock adapter")

	// Verify the temp file was removed by removeTempFile()
	_, err = os.Stat(tempPath)
	require.Error(t, err, "temp file should be removed after Store() failure")
}

func writeToFile(t *testing.T, ctx context.Context, storageID, namespace, filename string, content []byte) {
	t.Helper()
	f, err := fs.Create(ctx, storageID, namespace)
	require.NoError(t, err)

	n, err := f.Write(content)
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	require.NoError(t, f.Close())
	require.NoError(t, f.Store(ctx, filename))
}

func checkContent(t *testing.T, ctx context.Context, storageID, namespace string, filename string, content []byte) error {
	t.Helper()
	f, err := fs.Open(ctx, storageID, namespace, filename)
	if err != nil {
		t.Errorf("Failed to open namespace:%s filename:%s - %s", namespace, filename, err)
		return err
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(f)
	if err != nil {
		t.Errorf("Failed to read all namespace:%s filename:%s - %s", namespace, filename, err)
		return err
	}
	if !bytes.Equal(content, data) {
		t.Errorf("Content mismatch reading namespace:%s filename:%s", namespace, filename)
		return err
	}
	return nil
}

type mockEv struct{}

func (*mockEv) Touch(params.RelativePath) {}

func (*mockEv) Store(params.RelativePath, int64) bool {
	return true
}

// mockFailingAdapter is a mock adapter that fails on Put operations
type mockFailingAdapter struct {
	*mem.Adapter
}

func (m *mockFailingAdapter) Put(ctx context.Context, obj block.ObjectPointer, sizeBytes int64, reader io.Reader, opts block.PutOpts) (*block.PutResponse, error) {
	return nil, fmt.Errorf("mock adapter put failure")
}
