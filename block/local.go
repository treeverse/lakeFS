package block

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
)

type LocalFSAdapter struct {
	path string
	ctx  context.Context
}

func (l *LocalFSAdapter) WithContext(ctx context.Context) Adapter {
	return &LocalFSAdapter{
		path: l.path,
		ctx:  ctx,
	}
}

func NewLocalFSAdapter(path string) (Adapter, error) {
	stt, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !stt.IsDir() {
		return nil, fmt.Errorf("path provided is not a valid directory")
	}

	//if unix.Access(path, unix.W_OK) != nil {
	if !isDirectoryWritable(path) {
		return nil, fmt.Errorf("path provided is not writable")
	}
	return &LocalFSAdapter{path: path, ctx: context.Background()}, nil
}

func (l *LocalFSAdapter) getPath(identifier string) string {
	return path.Join(l.path, identifier)
}

func (l *LocalFSAdapter) Put(_ string, identifier string, _ int, reader io.Reader) error {
	path := l.getPath(identifier)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
	}()
	_, err = io.Copy(f, reader)
	return err
}

func (l *LocalFSAdapter) Get(_ string, identifier string) (reader io.ReadCloser, err error) {
	path := l.getPath(identifier)
	f, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (l *LocalFSAdapter) GetRange(_ string, identifier string, start int64, end int64) (io.ReadCloser, error) {
	path := l.getPath(identifier)
	f, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	_, err = f.Seek(start, 0)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func isDirectoryWritable(pth string) bool {
	// test ability to write to directory.
	// as there is no simple way to test this in windows, I prefer the "brute force" method
	// of creating s dummy file. will work in any OS.
	// speed is not an issue, as this will be activated very few times during startup

	fileName := path.Join(pth, "dummy.tmp")
	os.Remove(fileName)
	file, err := os.Create(fileName)
	if err == nil {
		file.Close()
		os.Remove(fileName)
		return true
	} else {
		return false
	}
}
