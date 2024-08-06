package fileutil

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultDirectoryMask = 0o755
)

var (
	ErrNotFile         = errors.New("path is not a file")
	ErrBadPath         = errors.New("bad path traversal blocked")
	ErrNotARegularFile = errors.New("not a regular file")
	ErrInvalidPath     = errors.New("invalid path")
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
	volumeName := filepath.VolumeName(fullPath)
	for fullPath != filepath.Join(volumeName, string(filepath.Separator)) {
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

func IsDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

// PruneEmptyDirectories iterates through the directory tree, removing empty directories, and directories that only
// contain empty directories.
func PruneEmptyDirectories(dirPath string) ([]string, error) {
	// Check if the directory exists
	info, err := os.Stat(dirPath)
	if err != nil {
		return nil, err
	}

	// Skip if it's not a directory
	if !info.IsDir() {
		return nil, nil
	}

	// Read the directory contents
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	// Recurse through the directory entries
	var pruned []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		subDirPath := filepath.Join(dirPath, entry.Name())
		prunedDirs, err := PruneEmptyDirectories(subDirPath)
		if err != nil {
			return nil, err
		}
		// Collect the pruned directories
		pruned = append(pruned, prunedDirs...)

		// Re-read the directory contents to check if it's empty now
		empty, err := IsDirEmpty(subDirPath)
		if err != nil {
			return nil, err
		}
		if empty {
			err = os.Remove(subDirPath)
			if err != nil {
				return nil, err
			}
			pruned = append(pruned, subDirPath)
		}
	}

	return pruned, nil
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
	if !filepath.IsAbs(absPath) || filepath.Clean(absPath) != absPath {
		return ErrBadPath
	}
	// point to storage namespace
	if !strings.HasPrefix(absPath, basePath) {
		return ErrInvalidPath
	}
	return nil
}

func VerifyRelPath(relPath, basePath string) error {
	abs := filepath.Join(basePath, relPath)
	return VerifyAbsPath(abs, basePath)
}

// VerifySafeFilename checks that the given file name is absolute and does not contain path traversal
func VerifySafeFilename(absPath string) error {
	if err := VerifyAbsPath(absPath, absPath); err != nil {
		return err
	}
	if !filepath.IsAbs(absPath) {
		return fmt.Errorf("relative path not allowed: %w", ErrInvalidPath)
	}
	return nil
}
