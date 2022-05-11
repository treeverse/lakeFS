package export

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

var (
	ErrEmptyFile       = errors.New("empty file")
	ErrBadHeader       = errors.New("bad header")
	ErrBadEntry        = errors.New("bad entry")
	ErrVersionMismatch = errors.New("version mismatch")
)

const (
	exportPrefix = "lakefs_export_"
)

type Entry struct {
	Key   []byte
	Value []byte
}

type Header struct {
	Version   uint32
	Timestamp time.Time
}

var (
	firstHeader *Header
	headerMu    sync.RWMutex
)

// TODO: (niro) implement export

// TODO: (niro) Make private after migration
func ImportFile(ctx context.Context, reader io.Reader, store kv.Store) error {
	jd := json.NewDecoder(reader)
	// Read header
	if !jd.More() {
		return ErrEmptyFile
	}
	var header Header
	err := jd.Decode(&header)
	// Decode does not return error on failure to Unmarshal
	if err != nil || header == (Header{}) {
		logging.Default().WithError(err).Error("Failed to decode header")
		return ErrBadHeader
	}
	err = validateHeader(header)
	if err != nil {
		return err
	}

	var entry Entry
	for jd.More() {
		err = jd.Decode(&entry)
		// Decode does not return error on failure to Unmarshal
		if err != nil || entry.Key == nil || entry.Value == nil {
			logging.Default().WithError(err).Error("Failed to decode entry")
			return ErrBadEntry
		}
		err = store.SetIf(ctx, entry.Key, entry.Value, nil)
		if err != nil {
			return err
		}
	}
	return err
}

func Import(ctx context.Context, store kv.Store, logger logging.Logger, path string) error {
	reader, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("error opening file (%s): %w", path, err)
	}
	defer reader.Close()

	pattern := exportPrefix + strings.Split(filepath.Base(path), ".tar.gz")[0]
	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		return fmt.Errorf("error creating temp dir (%s): %w", pattern, err)
	}
	defer os.RemoveAll(dir) // clean up

	err = Untar(dir, reader)
	if err != nil {
		return fmt.Errorf("error extracting files from tar (%s): %w", dir, err)
	}

	var files []string
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if info.Mode().IsRegular() {
			files = append(files, path)
		} else {
			logger.Warn("Ignoring irregular file. file:", path, "mode:", info.Mode())
		}
		return nil
	})

	var g multierror.Group
	for _, file := range files {
		f := file
		g.Go(func() error {
			fd, err := os.Open(f)
			if err != nil {
				return fmt.Errorf("error opening file (%s): %w", f, err)
			}
			defer fd.Close()
			err = ImportFile(ctx, fd, store)
			if err != nil {
				err = fmt.Errorf("failed to import file (%s): %w", f, err)
			}
			return err
		})
	}
	return g.Wait().ErrorOrNil()
}

func validateHeader(header Header) error {
	headerMu.Lock()
	defer headerMu.Unlock()
	if firstHeader == nil {
		firstHeader = new(Header)
		firstHeader.Version = header.Version
		firstHeader.Timestamp = header.Timestamp
		return nil
	}

	// TODO(niro): What do we want to validate in the headers?
	if firstHeader.Version != header.Version {
		return fmt.Errorf("first header %v, header %v: %w", firstHeader, header, ErrVersionMismatch)
	}
	return nil
}
