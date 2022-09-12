package local

import (
	"bytes"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/logging"
)

type EntriesIterator struct {
	start        []byte
	partitionKey []byte
	primed       bool

	entry *kv.Entry
	err   error
	iter  *badger.Iterator

	closer func()
	logger logging.Logger
}

func (e *EntriesIterator) Next() bool {
	start := time.Now()
	switch {
	case !e.primed && e.iter.Valid():
		e.primed = true
	case !e.primed:
		e.primed = true
		e.iter.Seek(e.start)
	default:
		e.iter.Next()
	}

	if !e.iter.Valid() {
		e.logger.Trace("no next values")
		return false
	}
	item := e.iter.Item()
	key := item.KeyCopy(nil)
	if !bytes.HasPrefix(key, e.partitionKey) {
		e.logger.WithField("next_key", string(key)).Trace("no next values with prefix")
		return false
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		e.logger.WithError(err).Trace("error reading value")
		e.err = err
	}
	e.entry = &kv.Entry{
		PartitionKey: e.partitionKey,
		Key:          key[len(partitionRange(e.partitionKey)):],
		Value:        value,
	}
	e.logger.WithField("next_key", string(key)).
		WithField("took", time.Since(start)).
		Trace("read next value")
	return true
}

func (e *EntriesIterator) Entry() *kv.Entry {
	return e.entry
}

func (e *EntriesIterator) Err() error {
	return e.err
}

func (e *EntriesIterator) Close() {
	e.closer()
}
