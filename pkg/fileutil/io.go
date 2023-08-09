package fileutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/karrick/godirwalk"
)

const (
	DefaultDirectoryMask = 0o755
)

// IsDir Returns true if p is a directory, otherwise false
func IsDir(p string) (bool, error) {
	stat, err := os.Stat(p)
	if err != nil {
		return false, err
	}
	return stat.IsDir(), nil
}

// FindInParents Returns the first occurrence of filename going up the dir tree
func FindInParents(dir, filename string) (string, error) {
	var lookup string
	fullPath, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for fullPath != string(filepath.Separator) && fullPath != filepath.VolumeName(fullPath) {
		info, err := os.Stat(fullPath)
		if err != nil {
			return "", fmt.Errorf("%s: %w", fullPath, err)
		}

		if !info.IsDir() {
			// find filename here
			lookup = filepath.Join(filepath.Dir(fullPath), filename)
		} else {
			lookup = filepath.Join(fullPath, filename)
		}
		_, err = os.Stat(lookup)
		if err == nil {
			return lookup, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
		// error == fs.ErrNotExist
		fullPath = filepath.Dir(fullPath)
	}
	return "", nil
}

func IsDirEmpty(dir string) (bool, error) {
	s, err := godirwalk.NewScanner(dir)
	if err != nil {
		return false, err
	}
	// Attempt to read only the first directory entry. Note that Scan skips both "." and ".." entries.
	hasAtLeastOneChild := s.Scan()
	if err = s.Err(); err != nil {
		return false, err
	}

	if hasAtLeastOneChild {
		return false, nil
	}
	return true, nil
}

// PruneEmptyDirectories iterates through the directory tree, removing empty directories, and directories that only
// contain empty directories.
func PruneEmptyDirectories(dir string) ([]string, error) {
	var pruned []string

	err := godirwalk.Walk(dir, &godirwalk.Options{
		Unsorted: true,
		Callback: func(_ string, _ *godirwalk.Dirent) error {
			// no-op while diving in; all the fun happens in PostChildrenCallback
			return nil
		},
		PostChildrenCallback: func(d string, _ *godirwalk.Dirent) error {
			empty, err := IsDirEmpty(d)
			if err != nil {
				return err
			}

			if d == dir || !empty { // do not remove top level directory or a directory with at least one child
				return nil
			}

			err = os.Remove(d)
			if err == nil {
				pruned = append(pruned, d)
			}
			return err
		},
	})

	return pruned, err
}
