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
	importWorkers  = 20
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
	return e.Je.Encode(v)
}

func importReader(ctx context.Context, log logging.Logger, jobChan chan<- *Entry, jd *json.Decoder) error {
	i := 0
	for {
		i++
		entry := new(Entry)
		err := jd.Decode(entry)
		if errors.Is(err, io.EOF) {
			return nil
		}
		// Decode does not return error on missing data / incompatible format
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
	// TODO(niro): Add validation to the header in the future
	log := logging.Default().WithFields(logging.Fields{
		"package_name":      header.PackageName,
		"lakefs_version":    header.LakeFSVersion,
		"db_schema_version": header.DBSchemaVersion,
		"created_at":        header.CreatedAt,
	})
	log.Info("Processing file")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	entryChan := make(chan *Entry, entryQueueSize)
	var g multierror.Group
	for i := 0; i < importWorkers; i++ {
		g.Go(func() error {
			for e := range entryChan {
				err := store.SetIf(ctx, e.PartitionKey, e.Key, e.Value, nil)
				if err != nil {
					cancel() // make sure reader will stop processing on first error
					return fmt.Errorf("import (partition key: %s, key: %s): %w", e.PartitionKey, e.Key, err)
				}
			}
			return nil
		})
	}
	readerErr := importReader(ctx, log, entryChan, jd)
	close(entryChan)

	err := g.Wait()
	err = multierror.Append(err, readerErr)
	return err.ErrorOrNil()
}
