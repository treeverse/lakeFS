package fileutil

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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

func FindInParents(path, filename string) (string, error) {
	var lookup string
	fullPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	for fullPath != string(filepath.Separator) && fullPath != filepath.VolumeName(fullPath) {
		info, err := os.Stat(fullPath)
		if errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("%s: %w", fullPath, fs.ErrNotExist)
		} else if err != nil {
			return "", err
		}
		if !info.IsDir() {
			// find filename here
			lookup = filepath.Join(filepath.Dir(fullPath), filename)
		} else {
			lookup = filepath.Join(fullPath, filename)
		}
		_, err = os.Stat(lookup)
		if os.IsNotExist(err) {
			fullPath = filepath.Dir(fullPath)
			continue
		} else if err != nil {
			return "", err
		}
		return lookup, nil
	}
	return "", nil
}
