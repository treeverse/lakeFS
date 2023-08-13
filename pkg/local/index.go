package local

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/treeverse/lakefs/pkg/fileutil"
	"github.com/treeverse/lakefs/pkg/uri"
	"gopkg.in/yaml.v3"
)

const (
	IndexFileName = ".lakefs_ref.yaml"
	IgnoreMarker  = "ignored by lakectl local:"
	IndexFileMode = 0644
)

// Index defines the structure of the lakefs local reference file
// consisting of the information linking local directory with lakefs path
type Index struct {
	root    string `yaml:"-"`
	PathURI string `yaml:"src"`
	AtHead  string `yaml:"at_head"`
}

func (l *Index) LocalPath() string {
	return l.root
}

func (l *Index) GetCurrentURI() (*uri.URI, error) {
	return uri.Parse(l.PathURI)
}

func WriteIndex(path string, remote *uri.URI, atHead string) (*Index, error) {
	idx := &Index{
		PathURI: remote.String(),
		AtHead:  atHead,
	}
	data, err := yaml.Marshal(idx)
	if err != nil {
		return nil, err
	}
	idxPath := filepath.Join(path, IndexFileName)
	return idx, os.WriteFile(idxPath, data, IndexFileMode)
}

func IndexExists(baseAbs string) (bool, error) {
	refPath := filepath.Join(baseAbs, IndexFileName)
	_, err := os.Stat(refPath)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}

func ReadIndex(path string) (*Index, error) {
	idxPath, err := fileutil.FindInParents(path, IndexFileName)
	if err != nil {
		return nil, err
	}
	if idxPath == "" {
		return nil, fmt.Errorf("could not find lakefs reference file in path %s or parents: %w", path, fs.ErrNotExist)
	}
	data, err := os.ReadFile(idxPath)
	if err != nil {
		return nil, err
	}
	idx := &Index{
		root: filepath.Dir(idxPath),
	}
	err = yaml.Unmarshal(data, idx)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

// FindIndices searches the specified root directory for index files, returning their relative directory paths while skipping hidden folders.
func FindIndices(root string) ([]string, error) {
	locs := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// don't traverse hidden folders like '.git', etc.
		if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		// if we found an index, no need to further traverse
		if filepath.Base(path) == IndexFileName {
			// add the relative location of the directory containing the index
			rel, err := filepath.Rel(root, filepath.Dir(path))
			if err != nil {
				return err
			}
			locs = append(locs, rel)
			return filepath.SkipDir // no need to traverse further!
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return locs, nil
}
