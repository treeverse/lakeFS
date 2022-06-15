package kv

import "bytes"

// SkipFirstIterator will keep the behaviour of the given EntriesIterator,
// except for skipping the first Entry if its Key is equal to 'after'.
type SkipFirstIterator struct {
	it         EntriesIterator
	after      []byte
	nextCalled bool
}

func (si *SkipFirstIterator) Next() bool {
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

func (si *SkipFirstIterator) Entry() *Entry {
	return si.it.Entry()
}

func (si *SkipFirstIterator) Err() error {
	return si.it.Err()
}

func (si *SkipFirstIterator) Close() {
	si.it.Close()
}

func NewSkipIterator(it EntriesIterator, after []byte) EntriesIterator {
	return &SkipFirstIterator{it: it, after: after}
}
