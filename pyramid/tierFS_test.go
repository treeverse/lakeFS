package pyramid

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/thanhpk/randstr"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/block/mem"
)

var (
	fs      FS
	adapter block.Adapter
)

const blockStoragePrefix = "prefix"
const allocatedDiskBytes = 4 * 1024 * 1024

func TestMain(m *testing.M) {
	fsName := uuid.New().String()

	// cleanup
	defer func() {
		if err := os.RemoveAll(path.Join(os.TempDir(), fsName)); err != nil {
			panic(err)
		}
	}()

	adapter = mem.New()
	var err error
	fs, err = NewFS(&Config{
		fsName:               fsName,
		adaptor:              adapter,
		fsBlockStoragePrefix: blockStoragePrefix,
		localBaseDir:         os.TempDir(),
		allocatedDiskBytes:   allocatedDiskBytes,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()

	os.Exit(code)
}

func TestSimpleWriteRead(t *testing.T) {
	namespace := uuid.New().String()
	filename := "1/2/file1.txt"

	content := "hello world!"
	writeToFile(t, namespace, filename, content)
	checkContent(t, namespace, filename, content)
}

func TestReadFailDuringWrite(t *testing.T) {
	namespace := uuid.New().String()
	filename := "file1"
	f, err := fs.Create(namespace)
	require.NoError(t, err)

	content := "some content"
	n, err := f.Write([]byte(content))
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	readF, err := fs.Open(namespace, filename)
	require.Nil(t, readF)
	require.Error(t, err)

	require.NoError(t, f.Close())
	require.NoError(t, f.store(filename))
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
	defer func() {
		if err := os.RemoveAll(path.Join(os.TempDir(), fsName)); err != nil {
			panic(err)
		}
	}()

	namespacePath := path.Join(os.TempDir(), fsName, namespace)
	workspacePath := path.Join(namespacePath, workspaceDir)
	if err := os.MkdirAll(workspacePath, os.ModePerm); err != nil {
		panic(err)
	}

	filename := "ThisShouldStay"
	content := "This Should Stay - I'm telling You!!!!"
	if err := ioutil.WriteFile(path.Join(namespacePath, filename), []byte(content), os.ModePerm); err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(path.Join(workspacePath, "ThisShouldNotStay"), []byte("ThisShouldNotStay"), os.ModePerm); err != nil {
		panic(err)
	}

	localFS, err := NewFS(&Config{
		fsName:               fsName,
		adaptor:              mem.New(),
		fsBlockStoragePrefix: blockStoragePrefix,
		localBaseDir:         os.TempDir(),
		allocatedDiskBytes:   allocatedDiskBytes,
	})
	if err != nil {
		panic(err)
	}

	dir, err := os.Open(workspacePath)
	require.Nil(t, dir)
	require.True(t, os.IsNotExist(err))

	f, err := localFS.Open(namespace, filename)
	defer f.Close()
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, content, string(bytes))

}

func testEviction(t *testing.T, namespaces ...string) {
	// making sure to fill the cache
	fileBytes := 512 * 1024
	numFiles := 5 * allocatedDiskBytes / fileBytes
	// write
	for i := 0; i < numFiles; i++ {
		filename := "file_" + strconv.Itoa(i)

		content := randstr.String(fileBytes, "abcdefghijklmnopqrstuvwxyz")
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

func writeToFile(t *testing.T, namespace, filename, content string) {
	f, err := fs.Create(namespace)
	require.NoError(t, err)

	n, err := f.Write([]byte(content))
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	require.NoError(t, f.Close())
	require.NoError(t, f.Store(filename))
}

func checkContent(t *testing.T, namespace string, filename string, content string) {
	f, err := fs.Open(namespace, filename)
	require.NoError(t, err)
	defer f.Close()

	bytes, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, content, string(bytes))
}
