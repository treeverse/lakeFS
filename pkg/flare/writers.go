package flare

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
)

type FlareOutputWriter interface {
	GetFileWriter(name string) (io.WriteCloser, error)
	Close() error
}

type GetFileWriterFunc func(name string) (io.WriteCloser, error)

type ZipWriter struct {
	*zip.Writer
}

type ZipWriterCloser struct {
	io.Writer
}

func (zwc ZipWriterCloser) Close() error {
	// noop
	return nil
}

func (zw *ZipWriter) GetFileWriter(name string) (io.WriteCloser, error) {
	f, err := zw.Create(name)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	zwc := ZipWriterCloser{f}

	return zwc, nil
}

func NewZipWriter(zipFileName string) (*ZipWriter, error) {
	fl, err := os.Create(zipFileName)
	if err != nil {
		return nil, err
	}

	w := zip.NewWriter(fl)

	return &ZipWriter{w}, nil
}

type FileWriter struct{}

func (fw *FileWriter) GetFileWriter(name string) (io.WriteCloser, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}

	return f, nil
}

func (fw *FileWriter) Close() error {
	// noop
	return nil
}

type StdoutWriter struct{}

type StdoutWriterCloser struct {
	io.Writer
}

func (swc StdoutWriterCloser) Close() error {
	// noop
	return nil
}

func (sw *StdoutWriter) GetFileWriter(name string) (io.WriteCloser, error) {
	fmt.Fprintf(os.Stdout, "\n// ------------------ %s ------------------\n", name)
	return StdoutWriterCloser{os.Stdout}, nil
}

func (sw *StdoutWriter) Close() error {
	// noop
	return nil
}
