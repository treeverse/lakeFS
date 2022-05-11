package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

var (
	ErrInvalidFormat   = errors.New("invalid format")
	ErrVersionMismatch = errors.New("version mismatch")
)

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
func Import(ctx context.Context, reader io.Reader, store Store) error {
	jd := json.NewDecoder(reader)
	// Read header
	if !jd.More() {
		return fmt.Errorf("%w: no content", ErrInvalidFormat)
	}
	var header Header
	err := jd.Decode(&header)
	// Decode does not return error on failure to Unmarshal
	if err != nil {
		return fmt.Errorf("%w: error decoding header", err)
	}
	if header == (Header{}) {
		return fmt.Errorf("%w: bad header format", ErrInvalidFormat)
	}
	if err = validateHeader(header); err != nil {
		return fmt.Errorf("header validation: %w", err)
	}

	var entry Entry
	for jd.More() {
		err = jd.Decode(&entry)
		// Decode does not return error on failure to Unmarshal
		if err != nil {
			return fmt.Errorf("%w: error decoding entry", err)
		}
		if entry.Key == nil || entry.Value == nil {
			return fmt.Errorf("%w: bad entry format", ErrInvalidFormat)
		}
		err = store.SetIf(ctx, entry.Key, entry.Value, nil)
		if err != nil {
			return err
		}
	}
	return nil
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
