package onboard

import (
	"github.com/treeverse/lakefs/catalog/rocks"
)

// prefixMergeIterator takes inventory iterator and commit iterator and merges them.
// If an entry's prefix is in `prefixes`, it would be taken from invIt.
// No entry from listIt would be returned if its prefix is in prefixes.
type prefixMergeIterator struct {
	invIt    rocks.EntryIterator
	listIt   rocks.EntryListingIterator
	prefixes []string

	err   error
	value *rocks.EntryRecord
}

func newPrefixMergeIterator(iterator rocks.EntryIterator, it rocks.EntryListingIterator, prefixes []string) rocks.EntryIterator {
	return &prefixMergeIterator{
		invIt:    iterator,
		listIt:   it,
		prefixes: prefixes,
		err:      nil,
		value:    nil,
	}
}

func (e *prefixMergeIterator) Next() bool {
	// TODO
	return false
}

func (e *prefixMergeIterator) SeekGE(_ rocks.Path) {
	// TODO
}

func (e *prefixMergeIterator) Value() *rocks.EntryRecord {
	// TODO
	return nil
}

func (e *prefixMergeIterator) Err() error {
	// TODO
	return nil
}

func (e *prefixMergeIterator) Close() {
	// TODO
	e.invIt.Close()
	e.err = e.invIt.Err()
}
