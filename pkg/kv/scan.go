package kv

import (
	"bytes"
	"context"
)

type PrefixIterator struct {
	Iterator  EntriesIterator
	Prefix    []byte
	completed bool
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
func ScanPrefix(ctx context.Context, store Store, prefix []byte) (EntriesIterator, error) {
	iter, err := store.Scan(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return &PrefixIterator{
		Iterator: iter,
		Prefix:   prefix,
	}, nil
}
