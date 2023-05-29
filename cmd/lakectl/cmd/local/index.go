package local

import (
	"errors"
	"fmt"
	"github.com/treeverse/lakefs/pkg/uri"
	"gopkg.in/yaml.v3"
	"io/fs"
	"os"
	"path/filepath"
)

const (
	PathSeparator = "/"
	IndexFileName = ".lakefs_ref.yaml"
	IndexFileMode = 0644
)

var (
	ErrLakeFSError   = errors.New("lakeFS error")
	ErrNoCommitFound = fmt.Errorf("%w: no commit found", ErrLakeFSError)
	ErrNoIndex       = errors.New("no lakefs_ref index found")
	ErrConflict      = errors.New("conflict")
	ErrChangeType    = errors.New("change type not supported")
)

type Index struct {
	root    string
	PathURI string `yaml:"src"`
	Head    string `yaml:"at_head"`
}

func (l *Index) LocalPath() string {
	return l.root
}

func (l *Index) GetCurrentURI() (*uri.URI, error) {
	return uri.Parse(l.PathURI)
}

func (l *Index) GetBaseURI() (*uri.URI, error) {
	current, err := l.GetCurrentURI()
	if err != nil {
		return nil, err
	}
	return WithRef(current, l.Head), nil
}

func FindIndices(root string) ([]string, error) {
	locs := make([]string, 0)
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	err = filepath.WalkDir(abs, func(path string, d fs.DirEntry, err error) error {
		// don't traverse hidden folders like '.git', etc.
		if d.IsDir() && d.Name()[0] == '.' {
			return filepath.SkipDir
		}
		// if we found an index, no need to further traverse
		if filepath.Base(path) == IndexFileName {
			// add the relative location of the directory containing the index
			rel, err := filepath.Rel(abs, filepath.Dir(path))
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

func ReadIndex(path string) (*Index, error) {
	idxPath, err := FindInParents(path, IndexFileName)
	if err != nil {
		return nil, err
	}
	if idxPath == "" {
		return nil, fmt.Errorf("%s: %w", path, ErrNoIndex)
	}
	data, err := os.ReadFile(idxPath)
	if err != nil {
		return nil, err
	}
	idx := &Index{}
	err = yaml.Unmarshal(data, idx)
	idx.root = filepath.Dir(idxPath)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func WriteIndex(path string, remote *uri.URI, atHead string) error {
	idx := &Index{
		PathURI: remote.String(),
		Head:    atHead,
	}
	data, err := yaml.Marshal(idx)
	if err != nil {
		return err
	}
	idxPath := filepath.Join(path, IndexFileName)
	return os.WriteFile(idxPath, data, IndexFileMode)
}
