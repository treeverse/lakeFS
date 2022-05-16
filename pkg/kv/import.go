package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/treeverse/lakefs/pkg/logging"
)

var ErrInvalidFormat = errors.New("invalid format")

// Header contains metadata information for import / export file
type Header struct {
	LakeFSVersion string
	PkgName       string
	DBVersion     uint32
	Timestamp     time.Time
}

// TODO: (niro) implement export

// TODO: (niro) Make private after migration
func Import(ctx context.Context, reader io.Reader, store Store) error {
	jd := json.NewDecoder(reader)
	// Read header
	var header Header
	if err := jd.Decode(&header); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: Empty file", ErrInvalidFormat)
		} else {
			return fmt.Errorf("%w: error decoding header", err)
		}
	}
	// Decode does not return error on failure to Unmarshal
	if header == (Header{}) {
		return fmt.Errorf("%w: bad header format", ErrInvalidFormat)
	}
	readHeader(header)

	var entry Entry
	for {
		err := jd.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		}
		// Decode does not return error on failure to Unmarshal
		if err != nil {
			return fmt.Errorf("%w: error decoding entry", err)
		}
		if len(entry.Key) == 0 {
			return fmt.Errorf("%w: bad entry key", ErrInvalidFormat)
		}
		if entry.Value == nil {
			return fmt.Errorf("%w: bad entry value", ErrInvalidFormat)
		}
		err = store.SetIf(ctx, entry.Key, entry.Value, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO(niro): Use this method to validate the header in the future
func readHeader(header Header) {
	logging.Default().WithFields(logging.Fields{
		"package name":   header.PkgName,
		"lakeFS version": header.LakeFSVersion,
		"DB Version":     header.DBVersion,
		"Timestamp":      header.Timestamp,
	}).Info("Processing file")
}
