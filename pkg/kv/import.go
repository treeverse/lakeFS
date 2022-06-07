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
	LakeFSVersion   string
	PackageName     string
	DBSchemaVersion int
	CreatedAt       time.Time
}

func Import(ctx context.Context, reader io.Reader, store Store) error {
	jd := json.NewDecoder(reader)
	// Read header
	var header Header
	if err := jd.Decode(&header); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("empty file: %w", ErrInvalidFormat)
		} else {
			return fmt.Errorf("decoding header: %w", err)
		}
	}
	// Decode does not return error onm missing data / incompatible format
	if header == (Header{}) {
		return fmt.Errorf("bad header format: %w", ErrInvalidFormat)
	}

	// TODO(niro): Add validation to the header in the future
	logging.Default().WithFields(logging.Fields{
		"package_name":      header.PackageName,
		"lakefs_version":    header.LakeFSVersion,
		"db_schema_version": header.DBSchemaVersion,
		"created_at":        header.CreatedAt,
	}).Info("Processing file")

	var entry Entry
	for {
		err := jd.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		}
		// Decode does not return error onm missing data / incompatible format
		if err != nil {
			return fmt.Errorf("decoding entry: %w", err)
		}
		if len(entry.PartitionKey) == 0 {
			return fmt.Errorf("bad entry partition key: %w", ErrInvalidFormat)
		}
		if len(entry.Key) == 0 {
			return fmt.Errorf("bad entry key: %w", ErrInvalidFormat)
		}
		if entry.Value == nil {
			return fmt.Errorf("bad entry value: %w", ErrInvalidFormat)
		}
		err = store.SetIf(ctx, entry.PartitionKey, entry.Key, entry.Value, nil)
		if err != nil {
			return err
		}
	}
	return nil
}
