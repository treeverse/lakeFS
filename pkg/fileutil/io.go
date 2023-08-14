package fileutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/karrick/godirwalk"
)

const (
	DefaultDirectoryMask = 0o755
)

var (
	ErrNotFile      = errors.New("path is not a file")
	ErrBadPath      = errors.New("bad path traversal blocked")
	ErrSymbolicLink = errors.New("symbolic links not supported")
	ErrInvalidPath  = errors.New("invalid path")
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

func RemoveFile(p string) error {
	fileExists, err := FileExists(p)
	if err != nil {
		return err
	}
	if !fileExists {
		return nil // does not exist
	}
	return os.Remove(p)
}

func FileExists(p string) (bool, error) {
	info, err := os.Stat(p)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if !info.IsDir() {
		return true, nil
	}
	return false, fmt.Errorf("%s: %w", p, ErrNotFile)
}

func VerifyAbsPath(absPath, basePath string) error {
	// check we have a valid abs path
	if !path.IsAbs(absPath) || path.Clean(absPath) != absPath {
		return ErrBadPath
	}
	// point to storage namespace
	if !strings.HasPrefix(absPath, basePath) {
		return ErrInvalidPath
	}
	return nil
}

func VerifyRelPath(relPath, basePath string) error {
	abs := basePath + string(os.PathSeparator) + relPath
	return VerifyAbsPath(abs, basePath)
}

// VerifySafeFilename checks that the given file name is not a symbolic link and that
// the file name does not contain path traversal
func VerifySafeFilename(absPath string) error {
	if err := VerifyAbsPath(absPath, absPath); err != nil {
		return err
	}
	if !filepath.IsAbs(absPath) {
		return fmt.Errorf("relative path not allowed: %w", ErrInvalidPath)
	}
	filename, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return err
	}
	if filename != absPath {
		return ErrSymbolicLink
	}
	return nil
}
