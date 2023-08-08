package kv

import (
	"bytes"
	"context"
)

type PrefixIterator struct {
	Iterator     EntriesIterator
	Prefix       []byte
	PartitionKey []byte
	completed    bool
}

func (b *PrefixIterator) Next() bool {
	if b.completed {
		return false
	}
	if !b.Iterator.Next() {
		b.completed = true
		return false
	}
	entry := b.Iterator.Entry()
	if !bytes.HasPrefix(entry.Key, b.Prefix) {
		b.completed = true
		return false
	}
	return true
}

func (b *PrefixIterator) Entry() *Entry {
	if b.completed {
		return nil
	}
	return b.Iterator.Entry()
}

func (b *PrefixIterator) Err() error {
	return b.Iterator.Err()
}

func (b *PrefixIterator) Close() {
	b.Iterator.Close()
}

// ScanPrefix returns an iterator on store that scan the set of keys that start with prefix
// after is the full key name for which to start the scan from
func ScanPrefix(ctx context.Context, store Store, partitionKey, prefix, after []byte) (EntriesIterator, error) {
	start := prefix
	if len(after) > 0 && string(after) > string(prefix) {
		start = after
	}
	iter, err := store.Scan(ctx, partitionKey, ScanOptions{
		KeyStart: start,
	})
	if err != nil {
		return nil, err
	}
	return &PrefixIterator{
		Iterator:     iter,
		Prefix:       prefix,
		PartitionKey: partitionKey,
	}, nil
}
