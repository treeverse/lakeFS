package block

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"golang.org/x/sys/unix"
)

type LocalFSAdapter struct {
	path string
}

func NewLocalFSAdapter(path string) (*LocalFSAdapter, error) {
	stt, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !stt.IsDir() {
		return nil, fmt.Errorf("path provided is not a valid directory")
	}
	if unix.Access(path, unix.W_OK) != nil {
		return nil, fmt.Errorf("path provided is not writable")
	}
	return &LocalFSAdapter{path: path}, nil
}

func (l *LocalFSAdapter) getPath(identifier string) string {
	return path.Join(l.path, identifier)
}

func (l *LocalFSAdapter) Put(block []byte, identifier string) error {
	path := l.getPath(identifier)
	return ioutil.WriteFile(path, block, 0755)
}

func (l *LocalFSAdapter) Get(identifier string) (block []byte, err error) {
	path := l.getPath(identifier)
	return ioutil.ReadFile(path)
}
