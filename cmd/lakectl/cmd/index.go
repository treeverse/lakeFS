package cmd

import (
	"errors"
	"os"
	"path/filepath"

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

func IndexExists(baseAbs string) bool {
	refPath := filepath.Join(baseAbs, IndexFileName)
	_, err := os.Stat(refPath)
	switch {
	case err == nil:
		return true
	case errors.Is(err, os.ErrNotExist):
		return false
	default:
		DieErr(err)
		return false // go fmt
	}
}
