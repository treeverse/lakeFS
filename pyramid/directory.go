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
	sync.Mutex
}

func (d *directory) deleteDirRecIfEmpty(dir string) error {
	d.Lock()
	defer d.Unlock()

	for dir != d.ceilingDir {
		empty, err := isDirEmpty(dir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if !empty {
			return nil
		}

		parentDir := path.Dir(dir)
		if err := os.Remove(dir); err != nil {
			return err
		}
		dir = parentDir
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

func (d *directory) createFile(path string) (*os.File, error) {
	d.Lock()
	defer d.Unlock()

	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return nil, fmt.Errorf("creating dir: %w", err)
	}

	return os.Create(path)
}

func (d *directory) renameFile(src, dst string) error {
	d.Lock()
	defer d.Unlock()

	if err := os.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
		return fmt.Errorf("creating dir: %w", err)
	}

	return os.Rename(src, dst)
}
