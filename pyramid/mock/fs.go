package mock

import "github.com/treeverse/lakefs/pyramid"

// FS is a mock implementation for pyramid.TierFS that uses a memory
// based FS that should be used only for testing.
type FS struct{}

func (fs *FS) Create(namespace string) (pyramid.StoredFile, error) {
	return &pyramid.WRFile{}, nil
}

func (fs *FS) Open(namespace, filename string) (pyramid.File, error) {
	return &pyramid.ROFile{}, nil
}
