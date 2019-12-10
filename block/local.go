package block

import (
	"fmt"
	"io"
	"os"
	"path"

	"golang.org/x/sys/unix"
)

type LocalFSAdapter struct {
	path string
}

func NewLocalFSAdapter(path string) (Adapter, error) {
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

func (l *LocalFSAdapter) Put(identifier string) (io.WriteCloser, error) {
	path := l.getPath(identifier)
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *LocalFSAdapter) Get(identifier string) (reader ReadAtCloser, err error) {
	path := l.getPath(identifier)
	f, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	return f, nil
}
