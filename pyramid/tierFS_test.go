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
const allocatedDiskBytes = 16 * 1024 * 1024
const estimatedFileBytes = 1 * 1024 * 1024

func TestMain(m *testing.M) {
	fsName := uuid.Must(uuid.NewRandom()).String()

	// cleanup
	defer func() {
		if err := os.RemoveAll(path.Join("/tmp", fsName)); err != nil {
			panic(err)
		}
	}()

	adapter = mem.New()
	var err error
	fs, err = NewFS(&Config{
		fsName:               fsName,
		adaptor:              adapter,
		fsBlockStoragePrefix: blockStoragePrefix,
		localBaseDir:         "/tmp",
		allocatedDiskBytes:   allocatedDiskBytes,
		estimatedFileBytes:   estimatedFileBytes,
	})
	if err != nil {
		panic(err)
	}

	code := m.Run()

	os.Exit(code)
}

func TestSimpleWriteRead(t *testing.T) {
	namespace := uuid.Must(uuid.NewRandom()).String()
	filename := "file1"

	content := "hello world!"
	writeToFile(t, namespace, filename, content)
	checkContent(t, namespace, filename, content)
}

func TestSimpleStoreRead(t *testing.T) {
	tempFilename := path.Join("/tmp/", uuid.Must(uuid.NewRandom()).String())
	namespace := uuid.Must(uuid.NewRandom()).String()
	filename := "file1"
	content := "hello world!"

	require.NoError(t, ioutil.WriteFile(tempFilename, []byte(content), os.ModePerm))
	require.NoError(t, fs.Store(namespace, tempFilename, filename))

	_, err := os.Stat(tempFilename)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))

	checkContent(t, namespace, filename, content)
}

func TestReadFailDuringWrite(t *testing.T) {
	namespace := uuid.Must(uuid.NewRandom()).String()
	filename := "file1"
	f, err := fs.Create(namespace, filename)
	require.NoError(t, err)

	content := "some content"
	n, err := f.Write([]byte(content))
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	readF, err := fs.Open(namespace, filename)
	require.Nil(t, readF)
	require.Error(t, err)

	require.NoError(t, f.Close())
	checkContent(t, namespace, filename, content)
}

func TestEvictionSingleNamespace(t *testing.T) {
	testEviction(t, uuid.Must(uuid.NewRandom()).String())
}

func TestEvictionMultipleNamespaces(t *testing.T) {
	testEviction(t, uuid.Must(uuid.NewRandom()).String(),
		uuid.Must(uuid.NewRandom()).String(),
		uuid.Must(uuid.NewRandom()).String())
}

func TestStartup(t *testing.T) {
	fsName := uuid.Must(uuid.NewRandom()).String()
	namespace := uuid.Must(uuid.NewRandom()).String()

	// cleanup
	defer func() {
		if err := os.RemoveAll(path.Join("/tmp", fsName)); err != nil {
			panic(err)
		}
	}()

	namespacePath := path.Join("/tmp", fsName, namespace)
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
		localBaseDir:         "/tmp",
		allocatedDiskBytes:   allocatedDiskBytes,
		estimatedFileBytes:   estimatedFileBytes,
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
	numFiles := 5 * allocatedDiskBytes / estimatedFileBytes
	// write
	for i := 0; i < numFiles; i++ {
		filename := "file_" + strconv.Itoa(i)

		content := randstr.String(estimatedFileBytes, "abcdefghijklmnopqrstuvwxyz")
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
	f, err := fs.Create("not/a/valid/namespace", "validfilename")
	require.Nil(t, f)
	require.Error(t, err)

	f, err = fs.Create("namespace", "not/a/valid/filename")
	require.Nil(t, f)
	require.Error(t, err)

	err = fs.Store("not/a/valid/namespace", "paths/are/valid/here", "filename")
	require.Error(t, err)

	err = fs.Store("namespace", "paths/are/valid/here", "not/a/valid/filename")
	require.Error(t, err)
}

func writeToFile(t *testing.T, namespace, filename, content string) {
	f, err := fs.Create(namespace, filename)
	require.NoError(t, err)

	n, err := f.Write([]byte(content))
	require.NoError(t, err)
	require.Equal(t, len(content), n)

	require.NoError(t, f.Close())
}

func checkContent(t *testing.T, namespace string, filename string, content string) {
	f, err := fs.Open(namespace, filename)
	defer f.Close()
	require.NoError(t, err)

	bytes, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, content, string(bytes))
}
