package pyramid

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
)

// directory synchronizes between file operations that might change (create/delete) directories
type directory struct {
	// ceilingDir is the root directory of the FS - shouldn't be deleted
	ceilingDir string
	mu         sync.Mutex
}

// deleteDirRecIfEmpty deletes the given directory if it is empty.
// It will continue to delete all parents directory if they are empty, until the ceilingDir.
// Passed dir path isn't checked for malicious referencing (e.g. "../../../usr") and should never be
// controlled by any user input.
func (d *directory) deleteDirRecIfEmpty(dir string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	for dir != d.ceilingDir {
		empty, err := isDirEmpty(dir)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !empty {
			return nil
		}

		if err := os.Remove(dir); err != nil {
			return err
		}
		// move up to the parent dir
		dir = path.Dir(dir)
	}
	return nil
}

func isDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = f.Close()
	}()

	_, err = f.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	return false, err
}

// renameFile will move the src file to dst location and creates all parent dirs if missing.
//
//	If we fail, we check if the destination exists, as it is content addressable, and remove the source.
func (d *directory) renameFile(src, dst string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	err := d.ensureParentDir(dst)
	if err != nil {
		return err
	}
	err = os.Rename(src, dst)
	if err != nil {
		// in case of an error we like to check if destination already exists and just remove the source.
		// this is usually relevant for cases where merge between two branches will have the same result as the source branch.
		// on Windows the destination file will be locked by one of the iterators that reading the left side of the merge and the
		// writer will try to store the result we are trying to rename.
		if _, err := os.Stat(dst); err == nil {
			return os.Remove(src)
		}
	}
	return err
}

func (d *directory) ensureParentDir(path string) error {
	parentDir := filepath.Dir(path)
	if parentDir == d.ceilingDir {
		return nil
	}
	err := os.MkdirAll(parentDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("creating dir: %w", err)
	}
	return nil
}
