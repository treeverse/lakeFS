package pyramid

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentCreateDeleteDir(t *testing.T) {
	name, err := ioutil.TempDir("", "test-dir-")
	require.NoError(t, err)
	defer os.RemoveAll(name) // clean up

	sut := directory{ceilingDir: name}

	var wg sync.WaitGroup
	concurrency := 1000
	pathDir := path.Join(name, "a/b/c/")

	for i := 0; i < concurrency; i++ {
		// create and delete
		filepath := path.Join(pathDir, strconv.Itoa(i))
		wg.Add(2)
		go func() {
			f, err := sut.createFile(filepath)
			require.NoError(t, err)
			require.NoError(t, f.Close())
			require.NoError(t, os.Remove(filepath))

			wg.Done()
		}()
		// delete folder - this will sometime succeed if the folder is empty
		go func() {
			require.NoError(t, sut.deleteDirRecIfEmpty(pathDir))

			wg.Done()
		}()
	}
	// It doesn't really matter if the dir exists and its content.
	// It's more about not panicking thru all of this
	wg.Wait()
}

func TestConcurrentRenameDeleteDir(t *testing.T) {
	name, err := ioutil.TempDir("", "test-dir-")
	require.NoError(t, err)
	defer os.RemoveAll(name) // clean up

	sut := directory{ceilingDir: name}

	var wg sync.WaitGroup
	concurrency := 1000
	pathDir := path.Join(name, "a/b/c/")

	for i := 0; i < concurrency; i++ {
		// create and delete
		originalPath := path.Join(name, strconv.Itoa(i))
		require.NoError(t, ioutil.WriteFile(originalPath, []byte("some data"), os.ModePerm))

		filepath := path.Join(pathDir, strconv.Itoa(i))
		wg.Add(2)
		go func() {
			err := sut.renameFile(originalPath, filepath)
			require.NoError(t, err)
			require.NoError(t, os.Remove(filepath))

			wg.Done()
		}()
		// delete folder - this will sometime succeed if the folder is empty
		go func() {
			require.NoError(t, sut.deleteDirRecIfEmpty(pathDir))

			wg.Done()
		}()
	}
	// It doesn't really matter if the dir exists and its content.
	// It's more about not panicking thru all of this
	wg.Wait()
}
