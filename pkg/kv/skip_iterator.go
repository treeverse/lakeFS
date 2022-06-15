package kv

import "bytes"

type SkipIterator struct {
	it         EntriesIterator
	after      []byte
	nextCalled bool
}

func (si *SkipIterator) Next() bool {
	if !si.nextCalled {
		si.nextCalled = true
		if !si.it.Next() {
			return false
		}
		if !bytes.Equal(si.it.Entry().Key, si.after) {
			return true
		}
	}
	return si.it.Next()
}

func (si *SkipIterator) Entry() *Entry {
	return si.it.Entry()
}

func (si *SkipIterator) Err() error {
	return si.it.Err()
}

func (si *SkipIterator) Close() {
	si.it.Close()
}

func NewSkipIterator(it EntriesIterator, after []byte) EntriesIterator {
	return &SkipIterator{it: it, after: after}
}
