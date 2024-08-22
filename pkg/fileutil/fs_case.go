package fileutil

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	nanoid "github.com/matoous/go-nanoid/v2"
)

// caseSensitiveNamePrefix is a prefix used for testing case sensitivity.
// It contains both lowercase and uppercase characters.
const caseSensitiveNamePrefix = ".cAsE."

// IsCaseInsensitiveLocation returns true if dirPath is a directory on a
// case-insensitive filesystem.  dirPath must be writable.  If it fails to
// delete the file, it uses warnFunc to emit a warning message.
func IsCaseInsensitiveLocation(fs FS, dirPath string, warnFunc func(string)) (bool, error) {
	// Random element in the filename to ensure uniqueness: the filename
	// will not be in use from anything else.
	id, err := nanoid.New()
	if err != nil {
		return false, fmt.Errorf("generate random name: %w", err)
	}
	exists, err := fs.Exists(dirPath)
	if err != nil {
		return false, fmt.Errorf("check path exists: %w", err)
	}
	if !exists { // Folder can be created by the command itself - go back one dir
		dirPath = filepath.Dir(dirPath)
	}
	path := filepath.Join(dirPath, caseSensitiveNamePrefix+id)
	err = fs.Touch(path)
	if err != nil {
		return false, fmt.Errorf("touch %s: %w", path, err)
	}
	defer func() {
		err := fs.Remove(path)
		if err != nil {
			warnFunc(fmt.Sprintf("Garbage file %s remains: %s; make sure to delete this file", path, err))
		}
	}()

	lowercasePrefix := strings.ToLower(caseSensitiveNamePrefix)
	lowercasePath := filepath.Join(dirPath, lowercasePrefix+id)
	// If lowercasePath exists, fs is case-insensitive.  Random id
	// ensures that it can be no other file!
	return fs.Exists(lowercasePath)
}

// FS is a tiny filesystem abstraction.  The standard io/fs does not support
// any write operations, see https://github.com/golang/go/issues/45757.
type FS interface {
	// Touch creates a file at path.
	Touch(path string) error
	// Exists returns true if there is a file at path.  It follows
	// symbolic links.
	Exists(path string) (bool, error)
	// Remove deletes the file at path.
	Remove(path string) error
}

// OSFS is the filesystem of the OS.
type OSFS struct{}

func NewOSFS() FS {
	return OSFS{}
}

func (OSFS) Touch(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	return file.Close()
}

func (OSFS) Exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (OSFS) Remove(path string) error {
	return os.Remove(path)
}
