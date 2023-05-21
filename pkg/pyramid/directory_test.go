package pyramid

import (
	"os"
	"path"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentRenameDeleteDir(t *testing.T) {
	name, err := os.MkdirTemp("", "test-dir-")
	require.NoError(t, err)
	defer os.RemoveAll(name) // clean up

	sut := directory{ceilingDir: name}

	var wg sync.WaitGroup
	concurrency := 1000
	pathDir := path.Join(name, "a/b/c/")

	for i := 0; i < concurrency; i++ {
		// create and delete
		originalPath := path.Join(name, strconv.Itoa(i))
		require.NoError(t, os.WriteFile(originalPath, []byte("some data"), os.ModePerm))

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
	// It's more about not panicking through all of this
	wg.Wait()
}
