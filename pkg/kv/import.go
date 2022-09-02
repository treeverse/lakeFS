package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/treeverse/lakefs/pkg/logging"
)

var ErrInvalidFormat = errors.New("invalid format")

const (
	importWorkers  = 10
	entryQueueSize = 100
)

// Header contains metadata information for import / export file
type Header struct {
	LakeFSVersion   string
	PackageName     string
	DBSchemaVersion int
	CreatedAt       time.Time
}

type SafeEncoder struct {
	Je *json.Encoder
	Mu sync.Mutex
}

func (e *SafeEncoder) Encode(v interface{}) error {
	e.Mu.Lock()
	defer e.Mu.Unlock()
	err := e.Je.Encode(v)
	if err != nil {
		return err
	}
	return nil
}

func consumer(ctx context.Context, cancel context.CancelFunc, jobChan <-chan *Entry, store Store) error {
	for {
		select {
		case e, more := <-jobChan:
			if !more {
				return nil
			}
			if len(e.PartitionKey) == 0 {
				cancel()
				return fmt.Errorf("bad entry partition key: %w", ErrInvalidFormat)
			}
			if len(e.Key) == 0 {
				cancel()
				return fmt.Errorf("bad entry key: %w", ErrInvalidFormat)
			}
			if e.Value == nil {
				cancel()
				return fmt.Errorf("bad entry value: %w", ErrInvalidFormat)
			}
			err := store.SetIf(ctx, e.PartitionKey, e.Key, e.Value, nil)
			if err != nil {
				cancel()
				return fmt.Errorf("import (partition key: %s, key: %s): %w", e.PartitionKey, e.Key, err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func producer(ctx context.Context, cancel context.CancelFunc, log logging.Logger, jobChan chan<- *Entry, jd *json.Decoder) error {
	i := 0
	for {
		i++
		entry := new(Entry)
		err := jd.Decode(entry)
		if errors.Is(err, io.EOF) {
			close(jobChan)
			return nil
		}
		// Decode does not return error on missing data / incompatible format
		if err != nil {
			cancel()
			return fmt.Errorf("decoding entry: %w", err)
		}
		if i%100_000 == 0 {
			log.Infof("Migrated %d entries", i)
		}

		select {
		case <-ctx.Done():
			return nil
		case jobChan <- entry:
			// Nothing to do
		}
	}
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
	// Decode does not return error on missing data / incompatible format
	if header == (Header{}) {
		return fmt.Errorf("bad header format: %w", ErrInvalidFormat)
	}
	cctx, cancel := context.WithCancel(ctx)
	// TODO(niro): Add validation to the header in the future
	log := logging.Default().WithFields(logging.Fields{
		"package_name":      header.PackageName,
		"lakefs_version":    header.LakeFSVersion,
		"db_schema_version": header.DBSchemaVersion,
		"created_at":        header.CreatedAt,
	})
	log.Info("Processing file")

	entryChan := make(chan *Entry, entryQueueSize)
	var g multierror.Group
	for i := 0; i < importWorkers; i++ {
		g.Go(func() error {
			return consumer(cctx, cancel, entryChan, store)
		})
	}
	g.Go(func() error {
		return producer(cctx, cancel, log, entryChan, jd)
	})

	return g.Wait().ErrorOrNil()
}
