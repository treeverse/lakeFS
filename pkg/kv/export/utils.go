package export

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/treeverse/lakefs/pkg/logging"
)

var ErrIllegalFilePath = errors.New("illegal file path")

// Untar takes a destination dir and a reader to a tar file.
// Looping over the tar file, extracting files into dir
func Untar(dir string, r io.Reader) error {
	gzr, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	for {
		header, err := tr.Next()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return err
		// Skip if the header is nil
		case header == nil:
			continue
		}
		path := filepath.Join(dir, header.Name) // nolint:gosec
		if !strings.HasPrefix(path, filepath.Clean(dir)) {
			logging.Default().WithError(ErrIllegalFilePath).Error("Illegal file path:", path)
			return ErrIllegalFilePath
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if _, err = os.Stat(path); err != nil {
				if err = os.MkdirAll(path, 0755); err != nil {
					return err
				}
			}
		case tar.TypeReg:
			fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			for {
				_, err := io.CopyN(fd, tr, 1024)
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
			}
			err = fd.Close()
			if err != nil {
				return err
			}
		}
	}
}
