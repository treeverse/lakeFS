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
	// ceilingDir is the root directory of the FS - shouldn't never be deleted
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
			return err
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
	defer f.Close()

	_, err = f.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	return false, err
}

// createFile creates the file under the path and creates all parent dirs if missing.
func (d *directory) createFile(path string) (*os.File, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.ensureParentDir(path); err != nil {
		return nil, err
	}

	return os.Create(path)
}

// renameFile will move the src file to dst location and creates all parent dirs if missing.
func (d *directory) renameFile(src, dst string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := d.ensureParentDir(dst); err != nil {
		return err
	}
	return os.Rename(src, dst)
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
